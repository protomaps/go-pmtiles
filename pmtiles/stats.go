package pmtiles

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/paulmach/protoscan"
	"github.com/schollz/progressbar/v3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"math"
	"golang.org/x/sync/errgroup"
	"runtime"
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

func calcStats(parts []uint64, data []byte) []byte {
	var buf bytes.Buffer
	reader := bytes.NewReader(data)

	for i := 0; i < len(parts); i += 2 {
		tile_id := parts[i]
		z, x, y := IdToZxy(tile_id)

		limit := &io.LimitedReader{reader, int64(parts[i+1])}
		gz_reader, err := gzip.NewReader(limit)
		if err != nil {
			panic(err)
		}
		decoded, err := ioutil.ReadAll(gz_reader)
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
				buf.WriteString(fmt.Sprintf("%d\t%d\t%d\t%s\t%d\t%d\t%d\n",z,x,y,name,features,attr_bytes,attr_values))
			default:
				msg.Skip()
			}
		}

		if msg.Err() != nil {
			panic(msg.Err())
		}
	}

	return buf.Bytes()
}

type Task struct {
	Index  int
	Parts []uint64 // N pairs of TileID, Length
	Data  []byte // N compressed tiles
	Result chan TaskResult
}

type TaskResult struct {
	Index int
	Rows  []byte
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

	// TODO: pre-determine all duplicate tiles.

	output, err := os.Create(file + ".layerstats.tsv.gz")
	if err != nil {
		panic(err)
	}
	defer output.Close()

	bar := progressbar.Default(
		int64(header.TileContentsCount),
		"writing stats",
	)

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	IO_BYTES := 10000000
	CHANNEL_SIZE := 10000
	TASK_TILE_CONTENTS := 1000

	f.Seek(int64(header.TileDataOffset), io.SeekStart)
	buffered := bufio.NewReaderSize(f, IO_BYTES)

	tasks := make(chan Task, CHANNEL_SIZE)
	intermediate := make(chan TaskResult, CHANNEL_SIZE)

	errs, _ := errgroup.WithContext(ctx)

	// workers
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			for task := range tasks {
				rows := calcStats(task.Parts, task.Data)
				task.Result <- TaskResult{task.Index, rows}
				bar.Add(len(task.Parts) / 2)
			}
			return nil
		})
	}

	// collector
	// buffers the results, outputting them in exact sorted order
	// once it has received all results, it terminates
	endTask := int(math.Ceil(float64(header.TileContentsCount) / float64(TASK_TILE_CONTENTS))) + 1
	fmt.Println(endTask)

	go func() {
		gzWriter, _ := gzip.NewWriterLevel(output,gzip.BestSpeed)
		defer gzWriter.Close()

		gzWriter.Write([]byte("hilbert,z,x,y,archive_tile_bytes,layer,layer_bytes,layer_features,attr_bytes,attr_values\n"))

		buffer := make(map[int]TaskResult)
		nextIndex := 0

		for i := range intermediate {
			buffer[i.Index] = i

			for {
				if next, ok := buffer[nextIndex]; ok {
					gzWriter.Write(next.Rows)

					delete(buffer, nextIndex)
					nextIndex++

					if nextIndex == endTask {
						close(intermediate)
					}
				} else {
					break
				}
			}
		}
	}()

	task_index := 0
	current_offset := uint64(0)
	parts := make([]uint64, 0)
	block_len := uint64(0)

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		// we only count unique tile entries
		if e.Offset == current_offset {
			parts = append(parts, e.TileId)
			parts = append(parts, uint64(e.Length))
			block_len += uint64(e.Length)

			if len(parts) == TASK_TILE_CONTENTS*2 {
				tilebytes := make([]byte, int64(block_len))
				_, err := io.ReadFull(buffered, tilebytes)
				if err != nil {
					panic(err)
				}
				tasks <- Task{Index: task_index, Parts:parts, Data: tilebytes, Result: intermediate}
				task_index += 1
				parts = nil
				block_len = 0
			}

			current_offset += uint64(e.Length)
		}
	})

	// flush the last part
	if block_len > 0  {
			tilebytes := make([]byte, int64(block_len))
			_, err := io.ReadFull(buffered, tilebytes)
			if err != nil {
				panic(err)
			}
			tasks <- Task{Index: task_index, Parts:parts, Data: tilebytes, Result: intermediate}
	}
	close(tasks)

	err = errs.Wait()
	if err != nil {
		return err
	}

	fmt.Printf("Completed stats in %v.\n", time.Since(start))
	return nil
}
