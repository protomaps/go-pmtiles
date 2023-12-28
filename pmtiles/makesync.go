package pmtiles

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Block struct {
	Index  uint64 // starts at 0
	Start  uint64 // the start tileID
	Offset uint64 // the offset in the file, in bytes
	Length uint64 // the length, in bytes
}

type Result struct {
	Block Block
	Hash  uint64
}

func Makesync(logger *log.Logger, file string, block_size_megabytes int) error {
	ctx := context.Background()
	start := time.Now()

	bucketURL, key, err := NormalizeBucketKey("", "", file)
	block_size_bytes := uint64(1024 * 1024 * block_size_megabytes)

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

	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])

	if !header.Clustered {
		return fmt.Errorf("Error: archive must be clustered for makesync.")
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

		directory := deserialize_entries(bytes.NewBuffer(b))
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

	output.Write([]byte("hash=fnv1a\n"))
	output.Write([]byte(fmt.Sprintf("blocksize=%d\n", block_size_bytes)))

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"writing syncfile",
	)

	var current Block

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

	tasks := make(chan Block, 10000)
	intermediate := make(chan Result, 10000)

	errs, _ := errgroup.WithContext(ctx)
	// workers
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			for block := range tasks {
				intermediate <- Result{block, GetHash(block.Offset, block.Length)}
			}
			return nil
		})
	}

	done := make(chan struct{})

	go func() {
		buffer := make(map[uint64]Result)
		nextIndex := uint64(0)

		for i := range intermediate {
			buffer[i.Block.Index] = i

			for {
				if next, ok := buffer[nextIndex]; ok {

					output.Write([]byte(fmt.Sprintf("%d\t%d\t%d\t%x\n", next.Block.Start, next.Block.Offset, next.Block.Length, next.Hash)))

					delete(buffer, nextIndex)
					nextIndex++

					if next.Block.Offset+next.Block.Length == header.TileDataLength {
						close(intermediate)
					}

				} else {
					break
				}
			}
		}

		done <- struct{}{}
	}()

	current_index := uint64(0)

	blocks := 0
	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)
		if current.Length == 0 {
			current.Index = current_index
			current.Start = e.TileId
			current.Offset = e.Offset
			current.Length = uint64(e.Length)
		} else if e.Offset < current.Offset+uint64(current.Length) { // todo: check max block length
			// ignore this entry
		} else if e.Offset > current.Offset+uint64(current.Length) {
			panic("Invalid clustering of archive detected - check with verify")
		} else {
			if current.Length+uint64(e.Length) > block_size_bytes {
				tasks <- Block{current.Index, current.Start, current.Offset, current.Length}
				blocks += 1

				current_index += 1
				current.Index = current_index
				current.Start = e.TileId
				current.Offset = e.Offset
				current.Length = uint64(e.Length)
			} else {
				current.Length += uint64(e.Length)
			}
		}
	})

	tasks <- Block{current.Index, current.Start, current.Offset, current.Length}
	blocks += 1
	close(tasks)

	<-done
	fmt.Printf("Created syncfile with %d blocks.\n", blocks)
	fmt.Printf("Completed makesync in %v.\n", time.Since(start))
	return nil
}

type Syncline struct {
	Offset uint64
	Length uint64
	Hash   uint64
}

func Sync(logger *log.Logger, file string, syncfile string) error {
	start := time.Now()
	total_remote_bytes := uint64(0)

	by_start_id := make(map[uint64]Syncline)

	sync, err := os.Open(syncfile)
	if err != nil {
		return fmt.Errorf("Error opening syncfile: %v\n", err)
	}
	defer sync.Close()
	scanner := bufio.NewScanner(sync)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 4 {
			continue
		}

		start_id, _ := strconv.ParseUint(parts[0], 10, 64)
		offset, _ := strconv.ParseUint(parts[1], 10, 64)
		length, _ := strconv.ParseUint(parts[2], 10, 64)
		total_remote_bytes += length
		hash, _ := strconv.ParseUint(parts[3], 16, 64)
		by_start_id[start_id] = Syncline{offset, length, hash}
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

	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])

	if !header.Clustered {
		return fmt.Errorf("Error: archive must be clustered for makesync.")
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

		directory := deserialize_entries(bytes.NewBuffer(b))
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

	total_blocks := len(by_start_id)
	hits := 0

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)

		potential_match, ok := by_start_id[e.TileId]
		if ok {
			hash_result := GetHash(e.Offset, potential_match.Length)
			if hash_result == potential_match.Hash {
				hits += 1
				delete(by_start_id, e.TileId)
			}
		}
	})

	to_transfer := uint64(0)
	for _, v := range by_start_id {
		to_transfer += v.Length
	}

	pct := float64(to_transfer) / float64(total_remote_bytes) * 100

	fmt.Printf("%d/%d blocks matched, need to transfer %s/%s (%.1f%%).\n", hits, total_blocks, humanize.Bytes(to_transfer), humanize.Bytes(total_remote_bytes), pct)

	fmt.Printf("Completed sync in %v.\n", time.Since(start))
	return nil
}
