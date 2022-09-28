package pmtiles

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"math"
)

type Compression uint8

const (
	UnknownCompression Compression = 0
	Gzip                           = 1
	Brotli                         = 2
	Zstd                           = 3
)

type TileType uint8

const (
	UnknownTileType TileType = 0
	Mvt                      = 1
	Png                      = 2
	Jpeg                     = 3
	Webp                     = 4
)

type HeaderV3 struct {
	SpecVersion         uint8
	RootOffset          uint64
	RootLength          uint64
	MetadataOffset      uint64
	MetadataLength      uint64
	LeafDirectoryOffset uint64
	LeafDirectoryLength uint64
	TileDataOffset      uint64
	TileDataLength      uint64
	AddressedTilesCount uint64
	TileEntriesCount    uint64
	TileContentsCount   uint64
	Clustered           bool
	InternalCompression Compression
	TileCompression     Compression
	TileType            TileType
	MinZoom             uint8
	MaxZoom             uint8
	MinLon              float32
	MinLat              float32
	MaxLon              float32
	MaxLat              float32
	CenterZoom          uint8
	CenterLon           float32
	CenterLat           float32
}

type EntryV3 struct {
	TileId    uint64
	Offset    uint64
	Length    uint32
	RunLength uint32
}

func serialize_entries(entries []EntryV3) bytes.Buffer {
	var b bytes.Buffer
	tmp := make([]byte, binary.MaxVarintLen64)
	w, _ := gzip.NewWriterLevel(&b, gzip.DefaultCompression)

	var n int
	n = binary.PutUvarint(tmp, uint64(len(entries)))
	w.Write(tmp[:n])

	lastId := uint64(0)
	for _, entry := range entries {
		n = binary.PutUvarint(tmp, uint64(entry.TileId)-lastId)
		w.Write(tmp[:n])

		lastId = uint64(entry.TileId)
	}

	for _, entry := range entries {
		n := binary.PutUvarint(tmp, uint64(entry.RunLength))
		w.Write(tmp[:n])
	}

	for _, entry := range entries {
		n := binary.PutUvarint(tmp, uint64(entry.Length))
		w.Write(tmp[:n])
	}

	for i, entry := range entries {
		var n int
		if i > 0 && entry.Offset == entries[i-1].Offset+uint64(entries[i-1].Length) {
			n = binary.PutUvarint(tmp, 0)
		} else {
			n = binary.PutUvarint(tmp, uint64(entry.Offset+1)) // add 1 to not conflict with 0
		}
		w.Write(tmp[:n])
	}

	w.Close()
	return b
}

func deserialize_entries(data bytes.Buffer) []EntryV3 {
	entries := make([]EntryV3, 0)

	reader, _ := gzip.NewReader(&data)
	byte_reader := bufio.NewReader(reader)

	num_entries, _ := binary.ReadUvarint(byte_reader)

	last_id := uint64(0)
	for i := uint64(0); i < num_entries; i++ {
		tmp, _ := binary.ReadUvarint(byte_reader)
		entries = append(entries, EntryV3{last_id + tmp, 0, 0, 0})
		last_id = last_id + tmp
	}

	for i := uint64(0); i < num_entries; i++ {
		run_length, _ := binary.ReadUvarint(byte_reader)
		entries[i].RunLength = uint32(run_length)
	}

	for i := uint64(0); i < num_entries; i++ {
		length, _ := binary.ReadUvarint(byte_reader)
		entries[i].Length = uint32(length)
	}

	for i := uint64(0); i < num_entries; i++ {
		tmp, _ := binary.ReadUvarint(byte_reader)
		if i > 0 && tmp == 0 {
			entries[i].Offset = entries[i-1].Offset + uint64(entries[i-1].Length)
		} else {
			entries[i].Offset = tmp - 1
		}
	}

	return entries
}

func serialize_header(header HeaderV3) []byte {
	b := make([]byte, 122)
	copy(b[0:2], "PM")

	b[2] = 3
	binary.LittleEndian.PutUint64(b[3:3+8], header.RootOffset)
	binary.LittleEndian.PutUint64(b[11:11+8], header.RootLength)
	binary.LittleEndian.PutUint64(b[19:19+8], header.MetadataOffset)
	binary.LittleEndian.PutUint64(b[27:27+8], header.MetadataLength)
	binary.LittleEndian.PutUint64(b[35:35+8], header.LeafDirectoryOffset)
	binary.LittleEndian.PutUint64(b[43:43+8], header.LeafDirectoryLength)
	binary.LittleEndian.PutUint64(b[51:51+8], header.TileDataOffset)
	binary.LittleEndian.PutUint64(b[59:59+8], header.TileDataLength)
	binary.LittleEndian.PutUint64(b[67:67+8], header.AddressedTilesCount)
	binary.LittleEndian.PutUint64(b[75:75+8], header.TileEntriesCount)
	binary.LittleEndian.PutUint64(b[83:83+8], header.TileContentsCount)
	if header.Clustered {
		b[91] = 0x1
	}
	b[92] = uint8(header.InternalCompression)
	b[93] = uint8(header.TileCompression)
	b[94] = uint8(header.TileType)
	b[95] = header.MinZoom
	b[96] = header.MaxZoom
	binary.LittleEndian.PutUint32(b[97:97+4], math.Float32bits(header.MinLon))
	binary.LittleEndian.PutUint32(b[101:101+4], math.Float32bits(header.MinLat))
	binary.LittleEndian.PutUint32(b[105:105+4], math.Float32bits(header.MaxLon))
	binary.LittleEndian.PutUint32(b[109:109+4], math.Float32bits(header.MaxLat))
	b[113] = header.CenterZoom
	binary.LittleEndian.PutUint32(b[114:114+4], math.Float32bits(header.CenterLon))
	binary.LittleEndian.PutUint32(b[118:118+4], math.Float32bits(header.CenterLat))
	return b
}

func deserialize_header(d []byte) HeaderV3 {
	h := HeaderV3{}
	h.RootOffset = binary.LittleEndian.Uint64(d[3 : 3+8])
	h.RootLength = binary.LittleEndian.Uint64(d[11 : 11+8])
	h.MetadataOffset = binary.LittleEndian.Uint64(d[19 : 19+8])
	h.MetadataLength = binary.LittleEndian.Uint64(d[27 : 27+8])
	h.LeafDirectoryOffset = binary.LittleEndian.Uint64(d[35 : 35+8])
	h.LeafDirectoryLength = binary.LittleEndian.Uint64(d[43 : 43+8])
	h.TileDataOffset = binary.LittleEndian.Uint64(d[51 : 51+8])
	h.TileDataLength = binary.LittleEndian.Uint64(d[59 : 59+8])
	h.AddressedTilesCount = binary.LittleEndian.Uint64(d[67 : 67+8])
	h.TileEntriesCount = binary.LittleEndian.Uint64(d[75 : 75+8])
	h.TileContentsCount = binary.LittleEndian.Uint64(d[83 : 83+8])
	h.Clustered = (d[91] == 0x1)
	h.InternalCompression = Compression(d[92])
	h.TileCompression = Compression(d[93])
	h.TileType = TileType(d[94])
	h.MinZoom = d[95]
	h.MaxZoom = d[96]
	h.MinLon = math.Float32frombits(binary.LittleEndian.Uint32(d[97 : 97+4]))
	h.MinLat = math.Float32frombits(binary.LittleEndian.Uint32(d[101 : 101+4]))
	h.MaxLon = math.Float32frombits(binary.LittleEndian.Uint32(d[105 : 105+4]))
	h.MaxLat = math.Float32frombits(binary.LittleEndian.Uint32(d[109 : 109+4]))
	h.CenterZoom = d[113]
	h.CenterLon = math.Float32frombits(binary.LittleEndian.Uint32(d[114 : 114+4]))
	h.CenterLat = math.Float32frombits(binary.LittleEndian.Uint32(d[118 : 118+4]))

	return h
}
