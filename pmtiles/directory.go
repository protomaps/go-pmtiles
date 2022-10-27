package pmtiles

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
)

type Compression uint8

const (
	UnknownCompression Compression = 0
	NoCompression                  = 1
	Gzip                           = 2
	Brotli                         = 3
	Zstd                           = 4
)

type TileType uint8

const (
	UnknownTileType TileType = 0
	Mvt                      = 1
	Png                      = 2
	Jpeg                     = 3
	Webp                     = 4
)

const HEADERV3_LEN_BYTES = 127

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
	MinLonE7            int32
	MinLatE7            int32
	MaxLonE7            int32
	MaxLatE7            int32
	CenterZoom          uint8
	CenterLonE7         int32
	CenterLatE7         int32
}

func headerContentType(header HeaderV3) (string, bool) {
	switch header.TileType {
	case Mvt:
		return "application/x-protobuf", true
	case Png:
		return "image/png", true
	case Jpeg:
		return "image/jpeg", true
	case Webp:
		return "image/webp", true
	default:
		return "", false
	}
}

func headerContentEncoding(compression Compression) (string, bool) {
	switch compression {
	case Gzip:
		return "gzip", true
	case Brotli:
		return "br", true
	default:
		return "", false
	}
}

type EntryV3 struct {
	TileId    uint64
	Offset    uint64
	Length    uint32
	RunLength uint32
}

func serialize_entries(entries []EntryV3) []byte {
	var b bytes.Buffer
	tmp := make([]byte, binary.MaxVarintLen64)
	w, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)

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
	return b.Bytes()
}

func deserialize_entries(data *bytes.Buffer) []EntryV3 {
	entries := make([]EntryV3, 0)

	reader, _ := gzip.NewReader(data)
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

func find_tile(entries []EntryV3, tileId uint64) (EntryV3, bool) {
	m := 0
	n := len(entries) - 1
	for m <= n {
		k := (n + m) >> 1
		cmp := int64(tileId) - int64(entries[k].TileId)
		if cmp > 0 {
			m = k + 1
		} else if cmp < 0 {
			n = k - 1
		} else {
			return entries[k], true
		}
	}

	// at this point, m > n
	if n >= 0 {
		if entries[n].RunLength == 0 {
			return entries[n], true
		}
		if tileId-entries[n].TileId < uint64(entries[n].RunLength) {
			return entries[n], true
		}
	}
	return EntryV3{}, false
}

func serialize_header(header HeaderV3) []byte {
	b := make([]byte, HEADERV3_LEN_BYTES)
	copy(b[0:7], "PMTiles")

	b[7] = 3
	binary.LittleEndian.PutUint64(b[8:8+8], header.RootOffset)
	binary.LittleEndian.PutUint64(b[16:16+8], header.RootLength)
	binary.LittleEndian.PutUint64(b[24:24+8], header.MetadataOffset)
	binary.LittleEndian.PutUint64(b[32:32+8], header.MetadataLength)
	binary.LittleEndian.PutUint64(b[40:40+8], header.LeafDirectoryOffset)
	binary.LittleEndian.PutUint64(b[48:48+8], header.LeafDirectoryLength)
	binary.LittleEndian.PutUint64(b[56:56+8], header.TileDataOffset)
	binary.LittleEndian.PutUint64(b[64:64+8], header.TileDataLength)
	binary.LittleEndian.PutUint64(b[72:72+8], header.AddressedTilesCount)
	binary.LittleEndian.PutUint64(b[80:80+8], header.TileEntriesCount)
	binary.LittleEndian.PutUint64(b[88:88+8], header.TileContentsCount)
	if header.Clustered {
		b[96] = 0x1
	}
	b[97] = uint8(header.InternalCompression)
	b[98] = uint8(header.TileCompression)
	b[99] = uint8(header.TileType)
	b[100] = header.MinZoom
	b[101] = header.MaxZoom
	binary.LittleEndian.PutUint32(b[102:102+4], uint32(header.MinLonE7))
	binary.LittleEndian.PutUint32(b[106:106+4], uint32(header.MinLatE7))
	binary.LittleEndian.PutUint32(b[110:110+4], uint32(header.MaxLonE7))
	binary.LittleEndian.PutUint32(b[114:114+4], uint32(header.MaxLatE7))
	b[118] = header.CenterZoom
	binary.LittleEndian.PutUint32(b[119:119+4], uint32(header.CenterLonE7))
	binary.LittleEndian.PutUint32(b[123:123+4], uint32(header.CenterLatE7))
	return b
}

func deserialize_header(d []byte) (HeaderV3, error) {
	h := HeaderV3{}
	magic_number := d[0:7]
	if string(magic_number) != "PMTiles" {
		return h, fmt.Errorf("Magic number not detected. Are you sure this is a PMTiles archive?")
	}

	spec_version := d[7]
	if spec_version > uint8(3) {
		return h, fmt.Errorf("Archive is spec version %d, but this program only supports version 3: upgrade your pmtiles program.", spec_version)
	}

	h.SpecVersion = spec_version
	h.RootOffset = binary.LittleEndian.Uint64(d[8 : 8+8])
	h.RootLength = binary.LittleEndian.Uint64(d[16 : 16+8])
	h.MetadataOffset = binary.LittleEndian.Uint64(d[24 : 24+8])
	h.MetadataLength = binary.LittleEndian.Uint64(d[32 : 32+8])
	h.LeafDirectoryOffset = binary.LittleEndian.Uint64(d[40 : 40+8])
	h.LeafDirectoryLength = binary.LittleEndian.Uint64(d[48 : 48+8])
	h.TileDataOffset = binary.LittleEndian.Uint64(d[56 : 56+8])
	h.TileDataLength = binary.LittleEndian.Uint64(d[64 : 64+8])
	h.AddressedTilesCount = binary.LittleEndian.Uint64(d[72 : 72+8])
	h.TileEntriesCount = binary.LittleEndian.Uint64(d[80 : 80+8])
	h.TileContentsCount = binary.LittleEndian.Uint64(d[88 : 88+8])
	h.Clustered = (d[96] == 0x1)
	h.InternalCompression = Compression(d[97])
	h.TileCompression = Compression(d[98])
	h.TileType = TileType(d[99])
	h.MinZoom = d[100]
	h.MaxZoom = d[101]
	h.MinLonE7 = int32(binary.LittleEndian.Uint32(d[102 : 102+4]))
	h.MinLatE7 = int32(binary.LittleEndian.Uint32(d[106 : 106+4]))
	h.MaxLonE7 = int32(binary.LittleEndian.Uint32(d[110 : 110+4]))
	h.MaxLatE7 = int32(binary.LittleEndian.Uint32(d[114 : 114+4]))
	h.CenterZoom = d[118]
	h.CenterLonE7 = int32(binary.LittleEndian.Uint32(d[119 : 119+4]))
	h.CenterLatE7 = int32(binary.LittleEndian.Uint32(d[123 : 123+4]))

	return h, nil
}

func build_roots_leaves(entries []EntryV3, leaf_size int) ([]byte, []byte, int) {
	root_entries := make([]EntryV3, 0)
	leaves_bytes := make([]byte, 0)
	num_leaves := 0

	for idx := 0; idx <= len(entries); idx += leaf_size {
		num_leaves++
		end := idx + leaf_size
		if idx+leaf_size > len(entries) {
			end = len(entries)
		}
		serialized := serialize_entries(entries[idx:end])

		root_entries = append(root_entries, EntryV3{entries[idx].TileId, uint64(len(leaves_bytes)), uint32(len(serialized)), 0})
		leaves_bytes = append(leaves_bytes, serialized...)
	}

	root_bytes := serialize_entries(root_entries)
	return root_bytes, leaves_bytes, num_leaves

}

func optimize_directories(entries []EntryV3, target_root_len int) ([]byte, []byte, int) {
	test_root_bytes := serialize_entries(entries)

	// Case1: the entire directory fits into the target len
	if len(test_root_bytes) <= target_root_len {
		return test_root_bytes, make([]byte, 0), 0
	} else {

		// TODO: case 2: mixed tile entries/directory entries in root

		// case 3: root directory is leaf pointers only
		// use an iterative method, increasing the size of the leaf directory until the root fits
		leaf_size := 4096
		for {
			root_bytes, leaves_bytes, num_leaves := build_roots_leaves(entries, leaf_size)
			if len(root_bytes) <= target_root_len {
				return root_bytes, leaves_bytes, num_leaves
			}
			leaf_size *= 2
		}
	}
}
