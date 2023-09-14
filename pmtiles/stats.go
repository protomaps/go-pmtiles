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
	"runtime"
	"strconv"
	"time"
)

type Task struct {
	Index  int
	TileId uint64
	Data   []byte
	Result chan TaskResult
}

type TaskResult struct {
	Index int
	Rows  [][]string
}

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

func calcStats(tileid uint64, tilebytes []byte) [][]string {
	z, x, y := IdToZxy(tileid)
	gzreader, err := gzip.NewReader(bytes.NewReader(tilebytes))
	if err != nil {
		panic(err)
	}
	decoded, err := ioutil.ReadAll(gzreader)
	if err != nil {
		panic(err)
	}
	msg := protoscan.New(decoded)
	var m *protoscan.Message

	rows := make([][]string, 0)
	for msg.Next() {
		switch msg.FieldNumber() {
		case 3:
			m, err = msg.Message(m)
			if err != nil {
				panic(err)
			}
			name, features, attr_bytes, attr_values := Layer(m)
			rows = append(rows, []string{
				strconv.FormatUint(tileid, 10),
				strconv.FormatUint(uint64(z), 10),
				strconv.FormatUint(uint64(x), 10),
				strconv.FormatUint(uint64(y), 10),
				strconv.FormatUint(uint64(len(tilebytes)), 10),
				name,
				strconv.Itoa(len(m.Data)),
				strconv.Itoa(features),
				strconv.Itoa(attr_bytes),
				strconv.Itoa(attr_values),
			})

		default:
			msg.Skip()
		}
	}

	if msg.Err() != nil {
		panic(msg.Err())
	}

	return rows
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

	bar := progressbar.Default(
		int64(header.TileEntriesCount),
		"writing stats",
	)

	f, err := os.Open(file)
	defer f.Close()
	f.Seek(int64(header.TileDataOffset), io.SeekStart)
	buffered := bufio.NewReaderSize(f, 10000000)

	tasks := make(chan Task, 100000)
	intermediate := make(chan TaskResult, 100000)

	// workers
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go func() {
			for task := range tasks {
				rows := calcStats(task.TileId, task.Data)
				task.Result <- TaskResult{task.Index, rows}
			}
		}()
	}

	// collector
	// buffers the results, outputting them in exact sorted order
	// once it has received all results, it terminates
	lastTask := int(header.TileContentsCount) - 1
	go func() {
		gzWriter := gzip.NewWriter(output)
		defer gzWriter.Close()

		csvWriter := csv.NewWriter(gzWriter)
		csvWriter.Comma = '\t'
		defer csvWriter.Flush()
		if err := csvWriter.Write([]string{"hilbert", "z", "x", "y", "archive_tile_bytes", "layer", "layer_bytes", "layer_features", "attr_bytes", "attr_values"}); err != nil {
			panic(fmt.Errorf("Failed to write header to TSV: %v", err))
		}

		buffer := make(map[int]TaskResult)
		nextIndex := 0

		for i := range intermediate {
			buffer[i.Index] = i

			for {
				if next, ok := buffer[nextIndex]; ok {
					for _, row := range next.Rows {
						if err := csvWriter.Write(row); err != nil {
							panic(fmt.Errorf("Failed to write record to TSV: %v", err))
						}
					}

					delete(buffer, nextIndex)
					nextIndex++

					if nextIndex == lastTask {
						close(intermediate)
					}
				} else {
					break
				}
			}
		}
	}()

	idx := 0
	current_offset := uint64(0)
	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		bar.Add(1)
		if e.Offset == current_offset {
			tilebytes := make([]byte, int64(e.Length))
			_, err := io.ReadFull(buffered, tilebytes)
			if err != nil {
				panic(err)
			}
			tasks <- Task{Index: idx, TileId: e.TileId, Data: tilebytes, Result: intermediate}
			idx += 1
			current_offset += uint64(e.Length)
		}
	})

	fmt.Printf("Completed stats in %v.\n", time.Since(start))
	return nil
}
