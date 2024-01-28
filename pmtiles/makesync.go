package pmtiles

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type block struct {
	Index  uint64 // starts at 0
	Start  uint64 // the start tileID
	Offset uint64 // the offset in the file, in bytes
	Length uint64 // the length, in bytes
}

type result struct {
	Block block
	Hash  uint64
}

type syncline struct {
	Offset uint64
	Length uint64
	Hash   uint64
}

// Makesync generates a syncfile for an archive on disk. (experimental)
func Makesync(_ *log.Logger, cliVersion string, file string, blockSizeKb int, checksum string) error {
	ctx := context.Background()
	start := time.Now()

	bucketURL, key, err := NormalizeBucketKey("", "", file)
	blockSizeBytes := uint64(1000 * blockSizeKb)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, 16384)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Failed to read %s, %w", key, err)
	}
	r.Close()

	header, err := deserializeHeader(b[0:HeaderV3LenBytes])

	if !header.Clustered {
		return fmt.Errorf("archive must be clustered for makesync")
	}

	var CollectEntries func(uint64, uint64, func(EntryV3))

	CollectEntries = func(dir_offset uint64, dir_length uint64, f func(EntryV3)) {
		dirbytes, err := bucket.NewRangeReader(ctx, key, int64(dir_offset), int64(dir_length))
		if err != nil {
			panic(fmt.Errorf("I/O error"))
		}
		defer dirbytes.Close()
		b, err = io.ReadAll(dirbytes)
		if err != nil {
			panic(fmt.Errorf("I/O Error"))
		}

		directory := deserializeEntries(bytes.NewBuffer(b))
		for _, entry := range directory {
			if entry.RunLength > 0 {
				f(entry)
			} else {
				CollectEntries(header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length), f)
			}
		}
	}

	output, err := os.Create(file + ".sync")
	if err != nil {
		panic(err)
	}
	defer output.Close()
	output.Write([]byte(fmt.Sprintf("version=%s\n", cliVersion)))

	if checksum == "md5" {
		localfile, err := os.Open(file)
		if err != nil {
			panic(err)
		}
		defer localfile.Close()
		reader := bufio.NewReaderSize(localfile, 64*1024*1024)
		md5hasher := md5.New()
		if _, err := io.Copy(md5hasher, reader); err != nil {
			panic(err)
		}
		md5checksum := md5hasher.Sum(nil)
		fmt.Printf("Completed md5 in %v.\n", time.Since(start))
		output.Write([]byte(fmt.Sprintf("md5=%x\n", md5checksum)))
	}

	output.Write([]byte("hash=fnv1a\n"))
	output.Write([]byte(fmt.Sprintf("blocksize=%d\n", blockSizeBytes)))

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"writing syncfile",
	)

	var current block

	tasks := make(chan block, 1000)

	var wg sync.WaitGroup
	var mu sync.Mutex

	synclines := make(map[uint64]syncline)

	errs, _ := errgroup.WithContext(ctx)
	// workers
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			wg.Add(1)
			hasher := fnv.New64a()
			for block := range tasks {
				r, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+block.Offset), int64(block.Length))
				if err != nil {
					log.Fatal(err)
				}

				if _, err := io.Copy(hasher, r); err != nil {
					log.Fatal(err)
				}
				r.Close()

				sum64 := hasher.Sum64()
				mu.Lock()
				synclines[block.Start] = syncline{block.Offset, block.Length, sum64}
				mu.Unlock()

				hasher.Reset()
			}
			wg.Done()
			return nil
		})
	}

	currentIndex := uint64(0)

	blocks := 0
	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)
		if current.Length == 0 {
			current.Index = currentIndex
			current.Start = e.TileID
			current.Offset = e.Offset
			current.Length = uint64(e.Length)
		} else if e.Offset < current.Offset+uint64(current.Length) { // todo: check max block length
			// ignore this entry
		} else if e.Offset > current.Offset+uint64(current.Length) {
			panic("Invalid clustering of archive detected - check with verify")
		} else {
			if current.Length+uint64(e.Length) > blockSizeBytes {
				tasks <- block{current.Index, current.Start, current.Offset, current.Length}
				blocks++

				currentIndex++
				current.Index = currentIndex
				current.Start = e.TileID
				current.Offset = e.Offset
				current.Length = uint64(e.Length)
			} else {
				current.Length += uint64(e.Length)
			}
		}
	})

	tasks <- block{current.Index, current.Start, current.Offset, current.Length}
	blocks++
	close(tasks)

	wg.Wait()

	var keys []uint64
	for k := range synclines {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, k := range keys {
		syncline := synclines[k]
		output.Write([]byte(fmt.Sprintf("%d\t%d\t%d\t%x\n", k, syncline.Offset, syncline.Length, syncline.Hash)))
	}

	fmt.Printf("Created syncfile with %d blocks.\n", blocks)
	fmt.Printf("Completed makesync in %v.\n", time.Since(start))
	return nil
}

// Sync calculates the diff between an archive on disk and a syncfile. (experimental)
func Sync(_ *log.Logger, file string, syncfile string) error {
	start := time.Now()
	totalRemoteBytes := uint64(0)

	byStartID := make(map[uint64]syncline)

	sync, err := os.Open(syncfile)
	if err != nil {
		return fmt.Errorf("error opening syncfile: %v", err)
	}
	defer sync.Close()
	scanner := bufio.NewScanner(sync)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 4 {
			continue
		}

		startID, _ := strconv.ParseUint(parts[0], 10, 64)
		offset, _ := strconv.ParseUint(parts[1], 10, 64)
		length, _ := strconv.ParseUint(parts[2], 10, 64)
		totalRemoteBytes += length
		hash, _ := strconv.ParseUint(parts[3], 16, 64)
		byStartID[startID] = syncline{offset, length, hash}
	}

	// open the existing archive

	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey("", "", file)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, 16384)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Failed to read %s, %w", key, err)
	}
	r.Close()

	header, err := deserializeHeader(b[0:HeaderV3LenBytes])

	if !header.Clustered {
		return fmt.Errorf("archive must be clustered for makesync")
	}

	GetHash := func(offset uint64, length uint64) uint64 {
		hasher := fnv.New64a()
		r, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+offset), int64(length))
		if err != nil {
			log.Fatal(err)
		}

		if _, err := io.Copy(hasher, r); err != nil {
			log.Fatal(err)
		}
		r.Close()
		return hasher.Sum64()
	}

	var CollectEntries func(uint64, uint64, func(EntryV3))

	CollectEntries = func(dir_offset uint64, dir_length uint64, f func(EntryV3)) {
		dirbytes, err := bucket.NewRangeReader(ctx, key, int64(dir_offset), int64(dir_length))
		if err != nil {
			panic(fmt.Errorf("I/O error"))
		}
		defer dirbytes.Close()
		b, err = io.ReadAll(dirbytes)
		if err != nil {
			panic(fmt.Errorf("I/O Error"))
		}

		directory := deserializeEntries(bytes.NewBuffer(b))
		for _, entry := range directory {
			if entry.RunLength > 0 {
				f(entry)
			} else {
				CollectEntries(header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length), f)
			}
		}
	}

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"calculating diff",
	)

	totalBlocks := len(byStartID)
	hits := 0

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)

		potentialMatch, ok := byStartID[e.TileID]
		if ok {
			hashResult := GetHash(e.Offset, potentialMatch.Length)
			if hashResult == potentialMatch.Hash {
				hits++
				delete(byStartID, e.TileID)
			}
		}
	})

	toTransfer := uint64(0)
	for _, v := range byStartID {
		toTransfer += v.Length
	}

	blocksMatched := float64(hits) / float64(totalBlocks) * 100
	pct := float64(toTransfer) / float64(totalRemoteBytes) * 100

	fmt.Printf("%d/%d blocks matched (%.1f%%), need to transfer %s/%s (%.1f%%).\n", hits, totalBlocks, blocksMatched, humanize.Bytes(toTransfer), humanize.Bytes(totalRemoteBytes), pct)

	fmt.Printf("Completed sync in %v.\n", time.Since(start))
	return nil
}
