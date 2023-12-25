package pmtiles

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
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
	start := time.Now()
	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey("", "", file)
	max_block_bytes := uint64(block_size_megabytes * 1024 * 1024)

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

	// while we're developing this let's store the md5 in the file as well
	localfile, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer localfile.Close()
	reader := bufio.NewReader(localfile)
	md5hasher := md5.New()
	if _, err := io.Copy(md5hasher, reader); err != nil {
		panic(err)
	}
	md5checksum := md5hasher.Sum(nil)

	output.Write([]byte(fmt.Sprintf("md5=%x\n", md5checksum)))
	output.Write([]byte("hash=fnv1a\n"))
	output.Write([]byte(fmt.Sprintf("maxblocksize=%d\n", max_block_bytes)))

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"writing syncfile",
	)

	var current Block

	hasher := fnv.New64a()

	GetHash := func(offset uint64, length uint64) uint64 {
		hasher.Reset()
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
			if current.Length+uint64(e.Length) > max_block_bytes {
				tasks <- Block{current.Index, current.Start, current.Offset, current.Length}

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
	close(tasks)

	<-done
	fmt.Printf("Completed makesync in %v.\n", time.Since(start))
	return nil
}
