package pmtiles

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/paulmach/protoscan"
	"github.com/schollz/progressbar/v3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

// layer name, # features, attr bytes, # attr values
func Layer(msg *protoscan.Message) (string, int, int, int) {
	var name string
	var err error
	features := 0
	values := 0
	attr_bytes := 0
	var m *protoscan.Message
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1: // name
			name, err = msg.String()
			if err != nil {
				panic(err)
			}
		case 2: // feature
			features += 1
			msg.Skip()
		case 3: // key
			m, err = msg.Message(m)
			attr_bytes += len(m.Data)
		case 4: // values
			values += 1
			m, err = msg.Message(m)
			attr_bytes += len(m.Data)
		default:
			msg.Skip()
		}
	}

	return name, features, attr_bytes, values
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
	if err := csvWriter.Write([]string{"hilbert", "z", "x", "y", "archive_tile_bytes", "layer", "layer_bytes", "layer_features", "attr_bytes", "attr_values"}); err != nil {
		return fmt.Errorf("Failed to write header to TSV: %v", err)
	}

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"writing stats",
	)

	f, err := os.Open(file)
	defer f.Close()
	f.Seek(int64(header.TileDataOffset), io.SeekStart)
	buffered := bufio.NewReaderSize(f, 10000000)

	current_offset := uint64(0)
	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)
		z, x, y := IdToZxy(e.TileId)
		if e.Offset == current_offset {
			tilebytes := io.LimitReader(buffered, int64(e.Length))
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
			msg := protoscan.New(decoded)
			var m *protoscan.Message
			for msg.Next() {
				switch msg.FieldNumber() {
				case 3:
					m, err = msg.Message(m)
					if err != nil {
						panic(err)
					}
					name, features, attr_bytes, attr_values := Layer(m)
					row := []string{
						strconv.FormatUint(e.TileId, 10),
						strconv.FormatUint(uint64(z), 10),
						strconv.FormatUint(uint64(x), 10),
						strconv.FormatUint(uint64(y), 10),
						strconv.FormatUint(uint64(e.Length), 10),
						name,
						strconv.Itoa(len(m.Data)),
						strconv.Itoa(features),
						strconv.Itoa(attr_bytes),
						strconv.Itoa(attr_values),
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

			current_offset += uint64(e.Length)
		}
	})

	fmt.Printf("Completed stats in %v.\n", time.Since(start))
	return nil
}
