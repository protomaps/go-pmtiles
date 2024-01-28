package pmtiles

import (
	"encoding/binary"
	"io"
	"math"
)

// Zxy coordinates of a single tile (zoom, column, row)
type Zxy struct {
	Z uint8
	X uint32
	Y uint32
}

type rangeV2 struct {
	Offset uint64
	Length uint64
}

type directoryV2 struct {
	Entries map[Zxy]rangeV2
	LeafZ   uint8
	Leaves  map[Zxy]rangeV2
}

func (d directoryV2) SizeBytes() int {
	return 21*(len(d.Entries)+len(d.Leaves)) + 1
}

func readUint24(b []byte) uint32 {
	return (uint32(binary.LittleEndian.Uint16(b[1:3])) << 8) + uint32(b[0])
}

func readUint48(b []byte) uint64 {
	return (uint64(binary.LittleEndian.Uint32(b[2:6])) << 16) + uint64(uint32(binary.LittleEndian.Uint16(b[0:2])))
}

func getParentTile(tile Zxy, level uint8) Zxy {
	tileDiff := tile.Z - level
	x := math.Floor(float64(tile.X / (1 << tileDiff)))
	y := math.Floor(float64(tile.Y / (1 << tileDiff)))
	return Zxy{Z: level, X: uint32(x), Y: uint32(y)}
}

func parseEntryV2(b []byte) (uint8, Zxy, rangeV2) {
	zRaw := b[0]
	xRaw := b[1:4]
	yRaw := b[4:7]
	offsetRaw := b[7:13]
	lengthRaw := b[13:17]
	x := readUint24(xRaw)
	y := readUint24(yRaw)
	offset := readUint48(offsetRaw)
	length := uint64(binary.LittleEndian.Uint32(lengthRaw))
	if zRaw&0b10000000 == 0 {
		return 0, Zxy{Z: uint8(zRaw), X: uint32(x), Y: uint32(y)}, rangeV2{Offset: offset, Length: length}
	}
	leafZ := zRaw & 0b01111111
	return leafZ, Zxy{Z: leafZ, X: uint32(x), Y: uint32(y)}, rangeV2{Offset: offset, Length: length}
}

func parseDirectoryV2(dirBytes []byte) directoryV2 {
	theDir := directoryV2{Entries: make(map[Zxy]rangeV2), Leaves: make(map[Zxy]rangeV2)}
	var maxz uint8
	for i := 0; i < len(dirBytes)/17; i++ {
		leafZ, zxy, rng := parseEntryV2(dirBytes[i*17 : i*17+17])
		if leafZ == 0 {
			theDir.Entries[zxy] = rng
		} else {
			maxz = leafZ // todo check spec
			theDir.Leaves[zxy] = rng
		}
	}
	theDir.LeafZ = maxz
	return theDir
}

func parseHeaderV2(reader io.Reader) ([]byte, directoryV2) {
	magicNum := make([]byte, 2)
	io.ReadFull(reader, magicNum)
	version := make([]byte, 2)
	io.ReadFull(reader, version)
	metadataLenBytes := make([]byte, 4)
	io.ReadFull(reader, metadataLenBytes)
	metadataLen := binary.LittleEndian.Uint32(metadataLenBytes)
	rootDirLenBytes := make([]byte, 2)
	io.ReadFull(reader, rootDirLenBytes)
	rootDirLen := int(binary.LittleEndian.Uint16(rootDirLenBytes))
	metadataBytes := make([]byte, metadataLen)
	io.ReadFull(reader, metadataBytes)
	dirBytes := make([]byte, rootDirLen*17)
	io.ReadFull(reader, dirBytes)
	theDir := parseDirectoryV2(dirBytes)
	return metadataBytes, theDir
}
