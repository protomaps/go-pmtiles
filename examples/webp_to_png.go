package main

import (
	"github.com/protomaps/go-pmtiles/pmtiles"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	// "github.com/schollz/progressbar/v3"
)

type work struct {
	i         int
	tileID    uint64
	runLength uint32
	img       []byte
}

// convert an archive from webp to png
func main() {
	output, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()

	input, _ := os.Open(os.Args[1])
	defer input.Close()
	buf := make([]byte, pmtiles.HeaderV3LenBytes)
	_, err = input.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	header, err := pmtiles.DeserializeHeader(buf)
	if err != nil {
		log.Fatal(err)
	}

	inChan := make(chan work)
	outChan := make(chan work)

	// set up pool of workers
	var wg sync.WaitGroup
	n := runtime.GOMAXPROCS(0)
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			for j := range inChan {
				//j.img = doWork(j.img) // reuse buffer
				outChan <- j
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outChan)
	}()

	// read tiles from the input and send them to the input channel
	go func() {
		idx := 0
		err = pmtiles.IterateEntries(header,
			func(offset uint64, length uint64) ([]byte, error) {
				sr := io.NewSectionReader(input, int64(offset), int64(length))
				return io.ReadAll(sr)
			},
			func(e pmtiles.EntryV3) {
				sr := io.NewSectionReader(input, int64(header.TileDataOffset+e.Offset), int64(e.Length))
				buf, _ := io.ReadAll(sr)
				inChan <- work{idx, e.TileID, e.RunLength, buf}
				idx++
			})
		if err != nil {
			log.Fatal(err)
		}
		close(inChan)
	}()

	// write 16384 bytes
	output.Write(make([]byte, 16384))

	// write the metadata
	input.Seek(int64(header.MetadataOffset), io.SeekStart)
	io.CopyN(output, input, int64(header.MetadataLength))

	// collect output in-order, assembling a new directory and writing new tiles
	next := 0
	pending := map[int]work{}

	offset := 0
	var newEntries []pmtiles.EntryV3
	for j := range outChan {
		pending[j.i] = j
		for {
			result, ok := pending[next]
			if !ok {
				break
			}
			output.Write(result.img)
			newEntries = append(newEntries, pmtiles.EntryV3{result.tileID, uint64(offset), uint32(len(result.img)), result.runLength})
			offset += int(len(result.img))
			delete(pending, next)
			next++
		}
	}

	rootBytes, leavesBytes, _ := pmtiles.OptimizeDirectories(newEntries, 16384-pmtiles.HeaderV3LenBytes, pmtiles.Gzip)
	_, err = output.Write(leavesBytes)
	if err != nil {
		log.Fatal(err)
	}

	header.TileType = pmtiles.Png
	header.MetadataOffset = 16384
	header.TileDataOffset = 16384 + header.MetadataLength
	header.TileDataLength = uint64(offset)
	header.LeafDirectoryOffset = header.TileDataOffset + header.TileDataLength
	header.LeafDirectoryLength = uint64(len(leavesBytes))
	header.RootOffset = pmtiles.HeaderV3LenBytes
	header.RootLength = uint64(len(rootBytes))

	output.Seek(0, io.SeekStart)
	headerBytes := pmtiles.SerializeHeader(header)
	_, err = output.Write(headerBytes)
	if err != nil {
		log.Fatal(err)
	}

	_, err = output.Write(rootBytes)
	if err != nil {
		log.Fatal(err)
	}
}
