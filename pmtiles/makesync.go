package pmtiles

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type syncBlock struct {
	Start  uint64 // the start tileID of the block
	Offset uint64 // the offset in the source archive
	Length uint64 // the length of the block
	Hash   uint64 // the hash of the block
}

type syncHeader struct {
	Version   string
	BlockSize uint64
	HashType  string
	HashSize  uint8
	B3Sum     string `json:"b3sum,omitempty"`
	MD5Sum    string `json:"md5sum,omitempty"`
	NumBlocks int
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

func Makesync(logger *log.Logger, cliVersion string, file string, blockSizeKb int, b3sum string) error {
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

	sh := syncHeader{
		Version:   cliVersion,
		HashSize:  8,
		BlockSize: blockSizeBytes,
		HashType:  "xxh64",
		NumBlocks: len(blocks),
	}

	if len(b3sum) > 0 {
		sh.B3Sum = b3sum
	}

	syncHeaderBytes, err := json.Marshal(sh)

	output.Write(syncHeaderBytes)
	output.Write([]byte{'\n'})

	serializeSyncBlocks(output, blocks)

	fmt.Printf("Created syncfile with %d blocks.\n", len(blocks))
	fmt.Printf("Completed makesync in %v.\n", time.Since(start))
	return nil
}