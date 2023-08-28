package pmtiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

func Stats(logger *log.Logger, file string) error {
	start := time.Now()
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

	if header.TileType != Mvt {
		return fmt.Errorf("Stats only works on MVT vector tilesets.")
	}

	// Pass 1: through the entire entry set, finding all non-duplicated tiles.

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

	seen_once := roaring64.New()
	seen_twice := roaring64.New()
	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		if seen_once.Contains(e.Offset) {
			seen_twice.Add(e.Offset)
		}
		seen_once.Add(e.Offset)
	})

	seen_once.AndNot(seen_twice)
	fmt.Println("Non-duplicate tiles:", seen_once.GetCardinality())

	// pass 2: decompress and parse tiles in order.

	output, err := os.Create(file + ".stats.tsv.gz")
	if err != nil {
		return fmt.Errorf("Failed to create output")
	}
	defer output.Close()

	gzWriter := gzip.NewWriter(output)
	defer gzWriter.Close()

	csvWriter := csv.NewWriter(gzWriter)
	csvWriter.Comma = '\t'
	defer csvWriter.Flush()
	if err := csvWriter.Write([]string{"z", "x", "y", "bytes_compressed"}); err != nil {
		return fmt.Errorf("Failed to write header to TSV: %v", err)
	}

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		if seen_once.Contains(e.Offset) {
			z, x, y := IdToZxy(e.TileId)
			row := []string{strconv.FormatUint(uint64(z), 10), strconv.FormatUint(uint64(x), 10), strconv.FormatUint(uint64(y), 10), strconv.FormatUint(uint64(e.Length), 10)}
			if err := csvWriter.Write(row); err != nil {
				panic(fmt.Errorf("Failed to write record to TSV: %v", err))
			}
		}
	})

	fmt.Printf("Completed stats in %v.\n", time.Since(start))
	return nil
}
