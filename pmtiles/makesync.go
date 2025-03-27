package pmtiles

import (
	"bufio"
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
)

type syncBlock struct {
	Start  uint64 // the start tileID of the block
	Offset uint64 // the offset in the source archive. This is not serialized, only used in makesync.
	Length uint64 // the length of the block
	Hash   uint64 // the hash of the block
}

type syncHeader struct {
	Version   string `json:"version"`
	BlockSize uint64 `json:"block_size"`
	HashType  string `json:"hash_type"`
	HashSize  uint8  `json:"hash_size"`
	NumBlocks int    `json:"num_blocks"`
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

func Makesync(logger *log.Logger, cliVersion string, fileName string, blockSizeKb int) error {
	fmt.Println("WARNING: This is an experimental feature. Do not rely on this in production!")
	blockSizeBytes := uint64(1000 * blockSizeKb)

	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)

	if err != nil {
		return err
	}

	buf := make([]byte, 127)
	_, err = file.Read(buf)
	if err != nil {
		return err
	}

	header, err := DeserializeHeader(buf)
	if err != nil {
		return err
	}

	if !header.Clustered {
		return fmt.Errorf("archive must be clustered for makesync")
	}

	output, err := os.Create(fileName + ".sync")

	if err != nil {
		return err
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

	errs := new(errgroup.Group)

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			wg.Add(1)
			hasher := xxhash.New()
			for block := range tasks {
				r := io.NewSectionReader(file, int64(header.TileDataOffset+block.Offset), int64(block.Length))

				if _, err := io.Copy(hasher, r); err != nil {
					log.Fatal(err)
				}

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

	err = IterateEntries(header,
		func(offset uint64, length uint64) ([]byte, error) {
			return io.ReadAll(io.NewSectionReader(file, int64(offset), int64(length)))
		},
		func(e EntryV3) {
			bar.Add(1)
			if current.Length == 0 {
				current.Start = e.TileID
				current.Offset = e.Offset
				current.Length = uint64(e.Length)
			} else if e.Offset > current.Offset+uint64(current.Length) {
				panic("Invalid clustering of archive detected - check with verify")
			} else if e.Offset == current.Offset+uint64(current.Length) {
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

	if err != nil {
		return err
	}

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

	syncHeaderBytes, err := json.Marshal(sh)

	output.Write(syncHeaderBytes)
	output.Write([]byte{'\n'})

	serializeSyncBlocks(output, blocks)

	fmt.Printf("Created syncfile with %d blocks.\n", len(blocks))
	return nil
}
