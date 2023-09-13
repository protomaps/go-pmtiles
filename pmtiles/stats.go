package pmtiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/paulmach/protoscan"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

func Layer(msg *protoscan.Message) string {
	var name string
	var err error
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1: // name
			name, err = msg.String()
			if err != nil {
				panic(err)
			}
		default:
			msg.Skip()
		}
	}

	return name
}

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

	output, err := os.Create(file + ".layerstats.tsv.gz")
	if err != nil {
		return fmt.Errorf("Failed to create output")
	}
	defer output.Close()

	gzWriter := gzip.NewWriter(output)
	defer gzWriter.Close()

	csvWriter := csv.NewWriter(gzWriter)
	csvWriter.Comma = '\t'
	defer csvWriter.Flush()
	if err := csvWriter.Write([]string{"z", "x", "y", "gzipped_bytes", "layer", "layer_bytes"}); err != nil {
		return fmt.Errorf("Failed to write header to TSV: %v", err)
	}

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		z, x, y := IdToZxy(e.TileId)

		tilebytes, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+e.Offset), int64(e.Length))
		if err != nil {
			panic(err)
		}
		gzreader, err := gzip.NewReader(tilebytes)
		if err != nil {
			panic(err)
		}
		decoded, err := ioutil.ReadAll(gzreader)
		if err != nil {
			panic(err)
		}
		tilebytes.Close()
		msg := protoscan.New(decoded)
		var m *protoscan.Message
		for msg.Next() {
			switch msg.FieldNumber() {
			case 3:
				m, err = msg.Message(m)
				if err != nil {
					panic(err)
				}
				name := Layer(m)
				row := []string{
					strconv.FormatUint(uint64(z), 10),
					strconv.FormatUint(uint64(x), 10),
					strconv.FormatUint(uint64(y), 10),
					strconv.FormatUint(uint64(e.Length), 10),
					name,
					strconv.Itoa(len(m.Data)),
				}
				if err := csvWriter.Write(row); err != nil {
					panic(fmt.Errorf("Failed to write record to TSV: %v", err))
				}

			default:
				msg.Skip()
			}
		}

		if msg.Err() != nil {
			panic(msg.Err())
		}

	})

	fmt.Printf("Completed stats in %v.\n", time.Since(start))
	return nil
}
