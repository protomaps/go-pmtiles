package main

import (
	"bytes"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/image/webp"
	"image/png"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type work struct {
	i         int
	tileID    uint64
	runLength uint32
	img       []byte
}

// transcode a raster archive from WebP to PNG
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

	if header.TileType != pmtiles.Webp {
		log.Fatal("input must be WebP")
	}
	if header.TileCompression != pmtiles.NoCompression {
		log.Fatal("input must not have tile compression")
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
				img, err := webp.Decode(bytes.NewReader(j.img))
				if err != nil {
					log.Println(j.tileID)
					panic(err)
				}
				var pngBuf bytes.Buffer
				if err := png.Encode(&pngBuf, img); err != nil {
					panic(err)
				}
				j.img = pngBuf.Bytes()
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
				buf, err := io.ReadAll(sr)
				if err != nil {
					panic(err)
				}
				inChan <- work{idx, e.TileID, e.RunLength, buf}
				idx++
			})
		if err != nil {
			log.Fatal(err)
		}
		close(inChan)
	}()

	// write 16384 zero bytes.
	// the eheader and root directory are guaranteed to fit in this section.
	output.Write(make([]byte, 16384))

	// copy the metadata from the input.
	input.Seek(int64(header.MetadataOffset), io.SeekStart)
	io.CopyN(output, input, int64(header.MetadataLength))

	// read results from the output channel, buffering them in-order
	// assemble a new directory and write new tile data to disk
	next := 0
	pending := map[int]work{}

	bar := progressbar.Default(int64(header.TileContentsCount))

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
			bar.Add(1)
			next++
		}
	}

	// create the root and leaves from the new entries
	rootBytes, leavesBytes, _ := pmtiles.OptimizeDirectories(newEntries, 16384-pmtiles.HeaderV3LenBytes, header.InternalCompression)
	_, err = output.Write(leavesBytes)
	if err != nil {
		log.Fatal(err)
	}

	// assign new values for offsets and lengths
	header.TileType = pmtiles.Png
	header.MetadataOffset = 16384
	header.TileDataOffset = 16384 + header.MetadataLength
	header.TileDataLength = uint64(offset)
	header.LeafDirectoryOffset = header.TileDataOffset + header.TileDataLength
	header.LeafDirectoryLength = uint64(len(leavesBytes))
	header.RootOffset = pmtiles.HeaderV3LenBytes
	header.RootLength = uint64(len(rootBytes))

	// rewind the input and write the header and root directory
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
