package pmtiles

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"sort"
)

type Entry struct {
	zxy Zxy
	rng Range
}

type Writer struct {
	file         *os.File
	offset       uint64
	tiles        []Entry
	hashToOffset map[uint64]uint64
}

type EntryAscending []Entry

func (e EntryAscending) Len() int {
	return len(e)
}

func (e EntryAscending) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e EntryAscending) Less(i, j int) bool {
	if e[i].zxy.Z != e[j].zxy.Z {
		return e[i].zxy.Z < e[j].zxy.Z
	}
	if e[i].zxy.X != e[j].zxy.X {
		return e[i].zxy.X < e[j].zxy.X
	}
	return e[i].zxy.Y < e[j].zxy.Y
}

func NewWriter(path string) Writer {
	f, err := os.Create(path)
	var empty []byte
	var offset uint64
	offset = 512000
	empty = make([]byte, offset)
	_, err = f.Write(empty)
	if err != nil {
		panic("Write failed")
	}
	hashToOffset := make(map[uint64]uint64)
	return Writer{file: f, offset: offset, hashToOffset: hashToOffset}
}

// return (uint32(binary.LittleEndian.Uint16(b[1:3])) << 8) + uint32(b[0])

func writeUint24(i uint32) []byte {
	result := make([]byte, 3)
	binary.LittleEndian.PutUint16(result[1:3], uint16(i>>8&0xFFFF))
	result[0] = uint8(i & 0xFF)
	return result
}

func writeUint48(i uint64) []byte {
	result := make([]byte, 6)
	binary.LittleEndian.PutUint32(result[2:6], uint32(i>>16&0xFFFFFFFF))
	binary.LittleEndian.PutUint16(result[0:2], uint16(i&0xFFFF))
	return result
}

func (writer *Writer) writeEntry(entry Entry) {
	binary.Write(writer.file, binary.LittleEndian, uint8(entry.zxy.Z))
	writer.file.Write(writeUint24(entry.zxy.X))
	writer.file.Write(writeUint24(entry.zxy.Y))
	writer.file.Write(writeUint48(entry.rng.Offset))
	binary.Write(writer.file, binary.LittleEndian, uint32(entry.rng.Length))
}

func (writer *Writer) WriteTile(zxy Zxy, data []byte) {
	// TODO do gzip decompression here
	hsh := fnv.New64a()
	hsh.Write(data)
	tileHash := hsh.Sum64()

	existingOffset, ok := writer.hashToOffset[tileHash]

	if ok {
		writer.tiles = append(writer.tiles, Entry{zxy: zxy, rng: Range{Offset: existingOffset, Length: uint32(len(data))}})
	} else {
		writer.file.Write(data)
		writer.tiles = append(writer.tiles, Entry{zxy: zxy, rng: Range{Offset: writer.offset, Length: uint32(len(data))}})
		writer.hashToOffset[tileHash] = writer.offset
		writer.offset += uint64(len(data))
	}
}

func (writer *Writer) writeHeader(metadata []byte, numRootEntries int) {
	_, _ = writer.file.Write([]byte{0x50, 0x4D})                               // magic number
	_ = binary.Write(writer.file, binary.LittleEndian, uint16(1))              // version
	_ = binary.Write(writer.file, binary.LittleEndian, uint32(len(metadata)))  // metadata length
	_ = binary.Write(writer.file, binary.LittleEndian, uint16(numRootEntries)) // root dir entries
	_, _ = writer.file.Write(metadata)
}

func (writer *Writer) Finalize(metadata_bytes []byte) {
	if len(writer.tiles) < 21845 {
		_, _ = writer.file.Seek(0, 0)
		writer.writeHeader(metadata_bytes, len(writer.tiles))

		sort.Sort(EntryAscending(writer.tiles))

		for _, entry := range writer.tiles {
			writer.writeEntry(entry)
		}
	} else {
		panic("Leaf directories not supported")
	}
	writer.file.Close()
}
