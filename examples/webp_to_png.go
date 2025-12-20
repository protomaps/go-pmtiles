package main

import (
	"github.com/protomaps/go-pmtiles/pmtiles"
	"os"
	"log"
	"io"
	"sync"
	"runtime"
)

type job struct {
	i   int
	tileID uint64
	runLength uint32
	img []byte
}

func process(img []byte) []byte {
	return img
}

// convert an archive from webp to png
func main() {
	input, _ := os.Open(os.Args[1])
	defer input.Close()
	buf := make([]byte, pmtiles.HeaderV3LenBytes)
	_, err := input.Read(buf)
	if err != nil {
		log.Fatal(err)	
	}
	h, err := pmtiles.DeserializeHeader(buf)
	if err != nil {
		log.Fatal(err)
	}

	in := make(chan job)
	out := make(chan job)

	// set up pool of workers
	var wg sync.WaitGroup
	n := runtime.GOMAXPROCS(0)
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			for j := range in {
				j.img = process(j.img) // reuse buffer
				out <- j
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	// read tiles from the input and send them to the input channel
	go func() {
		idx := 0
		err = pmtiles.IterateEntries(h,
			func(offset uint64, length uint64) ([]byte, error) {
				input.Seek(int64(offset), io.SeekStart)
				return io.ReadAll(io.LimitReader(input, int64(length)))
			},
			func(e pmtiles.EntryV3) {
				input.Seek(int64(h.TileDataOffset + e.Offset), io.SeekStart)
				buf, _ := io.ReadAll(io.LimitReader(input, int64(e.Length)))
				in <- job{idx, e.TileID, e.RunLength, buf}
				idx += 1
			})
		if err != nil {
			log.Fatal(err)
		}
		close(in)
	}()


	// collect output in-order, assembling a new directory and writing new tiles
	next := 0
	pending := map[int]job{}

	for j := range out {
		pending[j.i] = j
		for {
			toProcess, ok := pending[next]
			if !ok {
				break
			}
			log.Println(toProcess.i)
			// w.Write(buf)
			delete(pending, next)
			next++
		}
	}
}