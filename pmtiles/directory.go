package pmtiles

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type Zxy struct {
	Z uint8
	X uint32
	Y uint32
}

// Compression is the compression algorithm applied to individual tiles (or none)
type Compression uint8

const (
	UnknownCompression Compression = 0
	NoCompression                  = 1
	Gzip                           = 2
	Brotli                         = 3
	Zstd                           = 4
)

// TileType is the format of individual tile contents in the archive.
type TileType uint8

const (
	UnknownTileType TileType = 0
	Mvt                      = 1
	Png                      = 2
	Jpeg                     = 3
	Webp                     = 4
	Avif                     = 5
)

// HeaderV3LenBytes is the fixed-size binary header size.
const HeaderV3LenBytes = 127

// HeaderV3 is a binary header for PMTiles specification version 3.
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

// HeaderJson is a human-readable representation of parts of the binary header
// that may need to be manually edited.
// Omitted parts are the responsibility of the generator program and not editable.
// The formatting is aligned with the TileJSON / MBTiles specification.
type HeaderJson struct {
	TileCompression string    `json:"tile_compression"`
	TileType        string    `json:"tile_type"`
	MinZoom         int       `json:"minzoom"`
	MaxZoom         int       `json:"maxzoom"`
	Bounds          []float64 `json:"bounds"`
	Center          []float64 `json:"center"`
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
	case Avif:
		return "image/avif", true
	default:
		return "", false
	}
}

func tileTypeToString(t TileType) string {
	switch t {
	case Mvt:
		return "mvt"
	case Png:
		return "png"
	case Jpeg:
		return "jpg"
	case Webp:
		return "webp"
	case Avif:
		return "avif"
	default:
		return ""
	}
}

func stringToTileType(t string) TileType {
	switch t {
	case "mvt":
		return Mvt
	case "png":
		return Png
	case "jpg":
		return Jpeg
	case "webp":
		return Webp
	case "avif":
		return Avif
	default:
		return UnknownTileType
	}
}

func headerExt(header HeaderV3) string {
	base := tileTypeToString(header.TileType)
	if base == "" {
		return ""
	}
	return "." + base
}

func compressionToString(compression Compression) (string, bool) {
	switch compression {
	case NoCompression:
		return "none", false
	case Gzip:
		return "gzip", true
	case Brotli:
		return "br", true
	case Zstd:
		return "zstd", true
	default:
		return "unknown", false
	}
}

func stringToCompression(s string) Compression {
	switch s {
	case "none":
		return NoCompression
	case "gzip":
		return Gzip
	case "br":
		return Brotli
	case "zstd":
		return Zstd
	default:
		return UnknownCompression
	}
}

func headerToJson(header HeaderV3) HeaderJson {
	compressionString, _ := compressionToString(header.TileCompression)
	return HeaderJson{
		TileCompression: compressionString,
		TileType:        tileTypeToString(header.TileType),
		MinZoom:         int(header.MinZoom),
		MaxZoom:         int(header.MaxZoom),
		Bounds:          []float64{float64(header.MinLonE7) / 10000000, float64(header.MinLatE7) / 10000000, float64(header.MaxLonE7) / 10000000, float64(header.MaxLatE7) / 10000000},
		Center:          []float64{float64(header.CenterLonE7) / 10000000, float64(header.CenterLatE7) / 10000000, float64(header.CenterZoom)},
	}
}

func headerToStringifiedJson(header HeaderV3) string {
	s, _ := json.MarshalIndent(headerToJson(header), "", "    ")
	return string(s)
}

// EntryV3 is an entry in a PMTiles spec version 3 directory.
type EntryV3 struct {
	TileID    uint64
	Offset    uint64
	Length    uint32
	RunLength uint32
}

type nopWriteCloser struct {
	*bytes.Buffer
}

func (w *nopWriteCloser) Close() error { return nil }

func SerializeMetadata(metadata map[string]interface{}, compression Compression) ([]byte, error) {
	jsonBytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	if compression == NoCompression {
		return jsonBytes, nil
	} else if compression == Gzip {
		var b bytes.Buffer
		w, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
		if err != nil {
			return nil, err
		}
		w.Write(jsonBytes)
		w.Close()
		return b.Bytes(), nil
	} else {
		return nil, errors.New("compression not supported")
	}
}

func DeserializeMetadataBytes(reader io.Reader, compression Compression) ([]byte, error) {
	var jsonBytes []byte
	var err error

	if compression == NoCompression {
		jsonBytes, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	} else if compression == Gzip {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		jsonBytes, err = io.ReadAll(gzipReader)
		if err != nil {
			return nil, err
		}
		gzipReader.Close()
	} else {
		return nil, errors.New("compression not supported")
	}

	return jsonBytes, nil
}

func DeserializeMetadata(reader io.Reader, compression Compression) (map[string]interface{}, error) {
	jsonBytes, err := DeserializeMetadataBytes(reader, compression)
	var metadata map[string]interface{}
	err = json.Unmarshal(jsonBytes, &metadata)

	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func SerializeEntries(entries []EntryV3, compression Compression) []byte {
	var b bytes.Buffer
	var w io.WriteCloser

	tmp := make([]byte, binary.MaxVarintLen64)
	if compression == NoCompression {
		w = &nopWriteCloser{&b}
	} else if compression == Gzip {
		w, _ = gzip.NewWriterLevel(&b, gzip.BestCompression)
	} else {
		panic("Compression not supported")
	}

	var n int
	n = binary.PutUvarint(tmp, uint64(len(entries)))
	w.Write(tmp[:n])

	lastID := uint64(0)
	for _, entry := range entries {
		n = binary.PutUvarint(tmp, uint64(entry.TileID)-lastID)
		w.Write(tmp[:n])

		lastID = uint64(entry.TileID)
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

func DeserializeEntries(data *bytes.Buffer, compression Compression) []EntryV3 {
	entries := make([]EntryV3, 0)

	var reader io.Reader

	if compression == NoCompression {
		reader = data
	} else if compression == Gzip {
		reader, _ = gzip.NewReader(data)
	} else {
		panic("Compression not supported")
	}
	byteReader := bufio.NewReader(reader)

	numEntries, _ := binary.ReadUvarint(byteReader)

	lastID := uint64(0)
	for i := uint64(0); i < numEntries; i++ {
		tmp, _ := binary.ReadUvarint(byteReader)
		entries = append(entries, EntryV3{lastID + tmp, 0, 0, 0})
		lastID = lastID + tmp
	}

	for i := uint64(0); i < numEntries; i++ {
		runLength, _ := binary.ReadUvarint(byteReader)
		entries[i].RunLength = uint32(runLength)
	}

	for i := uint64(0); i < numEntries; i++ {
		length, _ := binary.ReadUvarint(byteReader)
		entries[i].Length = uint32(length)
	}

	for i := uint64(0); i < numEntries; i++ {
		tmp, _ := binary.ReadUvarint(byteReader)
		if i > 0 && tmp == 0 {
			entries[i].Offset = entries[i-1].Offset + uint64(entries[i-1].Length)
		} else {
			entries[i].Offset = tmp - 1
		}
	}

	return entries
}

func findTile(entries []EntryV3, tileID uint64) (EntryV3, bool) {
	m := 0
	n := len(entries) - 1
	for m <= n {
		k := (n + m) >> 1
		cmp := int64(tileID) - int64(entries[k].TileID)
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
		if tileID-entries[n].TileID < uint64(entries[n].RunLength) {
			return entries[n], true
		}
	}
	return EntryV3{}, false
}

func SerializeHeader(header HeaderV3) []byte {
	b := make([]byte, HeaderV3LenBytes)
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

func DeserializeHeader(d []byte) (HeaderV3, error) {
	h := HeaderV3{}
	magicNumber := d[0:7]
	if string(magicNumber) != "PMTiles" {
		return h, fmt.Errorf("magic number not detected. confirm this is a PMTiles archive")
	}

	specVersion := d[7]
	if specVersion > uint8(3) {
		return h, fmt.Errorf("archive is spec version %d, but this program only supports version 3: upgrade your pmtiles program", specVersion)
	}

	h.SpecVersion = specVersion
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

func buildRootsLeaves(entries []EntryV3, leafSize int, compression Compression) ([]byte, []byte, int) {
	rootEntries := make([]EntryV3, 0)
	leavesBytes := make([]byte, 0)
	numLeaves := 0

	for idx := 0; idx < len(entries); idx += leafSize {
		numLeaves++
		end := idx + leafSize
		if idx+leafSize > len(entries) {
			end = len(entries)
		}
		serialized := SerializeEntries(entries[idx:end], compression)

		rootEntries = append(rootEntries, EntryV3{entries[idx].TileID, uint64(len(leavesBytes)), uint32(len(serialized)), 0})
		leavesBytes = append(leavesBytes, serialized...)
	}

	rootBytes := SerializeEntries(rootEntries, compression)
	return rootBytes, leavesBytes, numLeaves
}

func optimizeDirectories(entries []EntryV3, targetRootLen int, compression Compression) ([]byte, []byte, int) {
	if len(entries) < 16384 {
		testRootBytes := SerializeEntries(entries, compression)
		// Case1: the entire directory fits into the target len
		if len(testRootBytes) <= targetRootLen {
			return testRootBytes, make([]byte, 0), 0
		}
	}

	// TODO: case 2: mixed tile entries/directory entries in root

	// case 3: root directory is leaf pointers only
	// use an iterative method, increasing the size of the leaf directory until the root fits

	var leafSize float32
	leafSize = float32(len(entries)) / 3500

	if leafSize < 4096 {
		leafSize = 4096
	}

	for {
		rootBytes, leavesBytes, numLeaves := buildRootsLeaves(entries, int(leafSize), compression)
		if len(rootBytes) <= targetRootLen {
			return rootBytes, leavesBytes, numLeaves
		}
		leafSize *= 1.2
	}
}

func IterateEntries(header HeaderV3, fetch func(uint64, uint64) ([]byte, error), operation func(EntryV3)) error {
	var CollectEntries func(uint64, uint64) error

	CollectEntries = func(dir_offset uint64, dir_length uint64) error {
		data, err := fetch(dir_offset, dir_length)
		if err != nil {
			return err
		}

		directory := DeserializeEntries(bytes.NewBuffer(data), header.InternalCompression)
		for _, entry := range directory {
			if entry.RunLength > 0 {
				operation(entry)
			} else {
				CollectEntries(header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length))
			}
		}
		return nil
	}

	return CollectEntries(header.RootOffset, header.RootLength)
}
