package pmtiles

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type syncBlock struct {
	Start  uint64 // the start tileID of the block
	Offset uint64 // the offset in the source archive
	Length uint64 // the length of the block
	Hash   uint64 // the hash of the block
}

type syncMetadata struct {
	Version      string
	BlockSize    uint64
	HashType     string
	HashSize     uint8
	ChecksumType string
	Checksum     string
	NumBlocks    int
}

type syncTask struct {
	NewBlock  syncBlock
	OldOffset uint64
}

func serializeSyncBlocks(output io.Writer, blocks []syncBlock) {
	tmp := make([]byte, binary.MaxVarintLen64)
	var n int

	lastStartID := uint64(0)
	for _, block := range blocks {
		n = binary.PutUvarint(tmp, uint64(block.Start-lastStartID))
		output.Write(tmp[:n])
		n = binary.PutUvarint(tmp, uint64(block.Length))
		output.Write(tmp[:n])
		binary.LittleEndian.PutUint64(tmp, block.Hash)
		output.Write(tmp[0:8])

		lastStartID = block.Start
	}
}

func deserializeSyncBlocks(numBlocks int, reader *bufio.Reader) []syncBlock {
	blocks := make([]syncBlock, 0)

	lastStartID := uint64(0)
	offset := uint64(0)
	buf := make([]byte, 8)

	for i := 0; i < numBlocks; i++ {
		start, _ := binary.ReadUvarint(reader)
		length, _ := binary.ReadUvarint(reader)
		_, _ = io.ReadFull(reader, buf)
		blocks = append(blocks, syncBlock{Start: lastStartID + start, Offset: offset, Length: length, Hash: binary.LittleEndian.Uint64(buf)})

		lastStartID = lastStartID + start
		offset = offset + length
	}

	return blocks
}

func Makesync(logger *log.Logger, cliVersion string, file string, blockSizeKb int, checksum string) error {
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
		fmt.Printf("md5=%x\n", md5checksum)
	}

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"writing syncfile",
	)

	var current syncBlock

	tasks := make(chan syncBlock, 1000)

	var wg sync.WaitGroup
	var mu sync.Mutex

	blocks := make([]syncBlock, 0)

	errs, _ := errgroup.WithContext(ctx)

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			wg.Add(1)
			hasher := xxhash.New()
			for block := range tasks {
				r, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+block.Offset), int64(block.Length))
				if err != nil {
					log.Fatal(err)
				}

				if _, err := io.Copy(hasher, r); err != nil {
					log.Fatal(err)
				}
				r.Close()

				block.Hash = hasher.Sum64()

				mu.Lock()
				blocks = append(blocks, block)
				mu.Unlock()

				hasher.Reset()
			}
			wg.Done()
			return nil
		})
	}

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)
		if current.Length == 0 {
			current.Start = e.TileID
			current.Offset = e.Offset
			current.Length = uint64(e.Length)
		} else if e.Offset < current.Offset+uint64(current.Length) { // todo: check max block length
			// ignore this entry
		} else if e.Offset > current.Offset+uint64(current.Length) {
			panic("Invalid clustering of archive detected - check with verify")
		} else {
			// check this logic
			if current.Length+uint64(e.Length) > blockSizeBytes {
				tasks <- syncBlock{current.Start, current.Offset, current.Length, 0}

				current.Start = e.TileID
				current.Offset = e.Offset
				current.Length = uint64(e.Length)
			} else {
				current.Length += uint64(e.Length)
			}
		}
	})

	tasks <- syncBlock{current.Start, current.Offset, current.Length, 0}
	close(tasks)

	wg.Wait()

	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Start < blocks[j].Start })

	metadataBytes, err := json.Marshal(syncMetadata{
		Version:   cliVersion,
		HashSize:  8,
		BlockSize: blockSizeBytes,
		HashType:  "xxh64",
		NumBlocks: len(blocks),
	})

	output.Write(metadataBytes)
	output.Write([]byte{'\n'})

	serializeSyncBlocks(output, blocks)

	fmt.Printf("Created syncfile with %d blocks.\n", len(blocks))
	fmt.Printf("Completed makesync in %v.\n", time.Since(start))
	return nil
}

func Sync(logger *log.Logger, oldVersion string, newVersion string, dryRun bool) error {
	start := time.Now()

	client := &http.Client{}

	var bufferedReader *bufio.Reader
	if strings.HasPrefix(newVersion, "http") {
		req, err := http.NewRequest("GET", newVersion+".sync", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf(".sync file not found")
		}
		if err != nil {
			return err
		}
		bar := progressbar.DefaultBytes(
			resp.ContentLength,
			"downloading syncfile",
		)
		bufferedReader = bufio.NewReader(io.TeeReader(resp.Body, bar))
	} else {
		newFile, err := os.Open(newVersion + ".sync")
		if err != nil {
			return fmt.Errorf("error opening syncfile: %v", err)
		}
		defer newFile.Close()
		bufferedReader = bufio.NewReader(newFile)
	}

	var metadata syncMetadata
	jsonBytes, _ := bufferedReader.ReadSlice('\n')

	json.Unmarshal(jsonBytes, &metadata)

	blocks := deserializeSyncBlocks(metadata.NumBlocks, bufferedReader)

	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey("", "", oldVersion)

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

	bar := progressbar.Default(
		int64(len(blocks)),
		"calculating diff",
	)

	wanted := make([]syncBlock, 0)
	have := make([]syncBlock, 0)

	idx := 0

	tasks := make(chan syncTask, 1000)
	var wg sync.WaitGroup
	var mu sync.Mutex

	errs, _ := errgroup.WithContext(ctx)

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			wg.Add(1)
			for task := range tasks {
				hasher := xxhash.New()
				r, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+task.OldOffset), int64(task.NewBlock.Length))
				if err != nil {
					log.Fatal(err)
				}

				if _, err := io.Copy(hasher, r); err != nil {
					log.Fatal(err)
				}
				r.Close()

				mu.Lock()
				if task.NewBlock.Hash == hasher.Sum64() {
					have = append(have, task.NewBlock)
				} else {
					wanted = append(wanted, task.NewBlock)
				}
				mu.Unlock()
			}
			wg.Done()
			return nil
		})
	}

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		if idx < len(blocks) {
			for e.TileID > blocks[idx].Start {
				mu.Lock()
				wanted = append(wanted, blocks[idx])
				mu.Unlock()
				bar.Add(1)
				idx = idx + 1
			}

			if e.TileID == blocks[idx].Start {
				tasks <- syncTask{NewBlock: blocks[idx], OldOffset: e.Offset}
				bar.Add(1)
				idx = idx + 1
			}
		}
	})

	// we may not have consumed until the end
	for idx < len(blocks) {
		mu.Lock()
		wanted = append(wanted, blocks[idx])
		mu.Unlock()
		bar.Add(1)
		idx = idx + 1
	}

	close(tasks)
	wg.Wait()

	sort.Slice(wanted, func(i, j int) bool { return wanted[i].Start < wanted[j].Start })

	toTransfer := uint64(0)
	totalRemoteBytes := uint64(0)
	for _, v := range wanted {
		toTransfer += v.Length
		totalRemoteBytes += v.Length
	}

	for _, v := range have {
		totalRemoteBytes += v.Length
	}

	blocksMatched := float64(len(have)) / float64(len(blocks)) * 100
	pct := float64(toTransfer) / float64(totalRemoteBytes) * 100

	fmt.Printf("%d/%d blocks matched (%.1f%%), need to transfer %s/%s (%.1f%%).\n", len(have), len(blocks), blocksMatched, humanize.Bytes(toTransfer), humanize.Bytes(totalRemoteBytes), pct)

	ranges := make([]srcDstRange, 0)
	for _, v := range wanted {
		l := len(ranges)
		if l > 0 && (ranges[l-1].SrcOffset+ranges[l-1].Length) == v.Offset {
			ranges[l-1].Length = ranges[l-1].Length + v.Length
		} else {
			ranges = append(ranges, srcDstRange{SrcOffset: v.Offset, DstOffset: v.Offset, Length: v.Length})
		}
	}
	fmt.Printf("need %d chunks.\n", len(ranges))

	if !dryRun {
		req, err := http.NewRequest("GET", newVersion, nil)

		var rangeParts []string

		for _, r := range ranges {
			rangeParts = append(rangeParts, fmt.Sprintf("%d-%d", r.SrcOffset, r.SrcOffset+r.Length+1))
		}

		headerVal := strings.Join(rangeParts, ",")
		req.Header.Set("Range", fmt.Sprintf("bytes=%s", headerVal))
		resp, err := client.Do(req)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("non-OK multirange request")
		}

		_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil {
			return err
		}

		mr := multipart.NewReader(resp.Body, params["boundary"])

		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			_ = part.Header.Get("Content-Range")

			partBytes, err := io.ReadAll(part)
			if err != nil {
				return err
			}

			_ = partBytes
		}
	}

	fmt.Printf("Completed sync in %v.\n", time.Since(start))
	return nil
}
