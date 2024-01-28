package pmtiles

import (
	"encoding/binary"
	"io"
	"math"
)

type Zxy struct {
	Z uint8
	X uint32
	Y uint32
}

type Range struct {
	Offset uint64
	Length uint64
}

type DirectoryV2 struct {
	Entries map[Zxy]Range
	LeafZ   uint8
	Leaves  map[Zxy]Range
}

func (d DirectoryV2) SizeBytes() int {
	return 21*(len(d.Entries)+len(d.Leaves)) + 1
}

func readUint24(b []byte) uint32 {
	return (uint32(binary.LittleEndian.Uint16(b[1:3])) << 8) + uint32(b[0])
}

func readUint48(b []byte) uint64 {
	return (uint64(binary.LittleEndian.Uint32(b[2:6])) << 16) + uint64(uint32(binary.LittleEndian.Uint16(b[0:2])))
}

func GetParentTile(tile Zxy, level uint8) Zxy {
	tileDiff := tile.Z - level
	x := math.Floor(float64(tile.X / (1 << tileDiff)))
	y := math.Floor(float64(tile.Y / (1 << tileDiff)))
	return Zxy{Z: level, X: uint32(x), Y: uint32(y)}
}

func ParseEntryV2(b []byte) (uint8, Zxy, Range) {
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
		return 0, Zxy{Z: uint8(zRaw), X: uint32(x), Y: uint32(y)}, Range{Offset: offset, Length: length}
	} else {
		leafZ := zRaw & 0b01111111
		return leafZ, Zxy{Z: leafZ, X: uint32(x), Y: uint32(y)}, Range{Offset: offset, Length: length}
	}
}

func ParseDirectoryV2(dirBytes []byte) DirectoryV2 {
	theDir := DirectoryV2{Entries: make(map[Zxy]Range), Leaves: make(map[Zxy]Range)}
	var maxz uint8
	for i := 0; i < len(dirBytes)/17; i++ {
		leafZ, zxy, rng := ParseEntryV2(dirBytes[i*17 : i*17+17])
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

func ParseHeaderV2(reader io.Reader) ([]byte, DirectoryV2) {
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
	theDir := ParseDirectoryV2(dirBytes)
	return metadataBytes, theDir
}
