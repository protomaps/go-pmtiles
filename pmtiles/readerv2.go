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
	tile_diff := tile.Z - level
	x := math.Floor(float64(tile.X / (1 << tile_diff)))
	y := math.Floor(float64(tile.Y / (1 << tile_diff)))
	return Zxy{Z: level, X: uint32(x), Y: uint32(y)}
}

func ParseEntryV2(b []byte) (uint8, Zxy, Range) {
	z_raw := b[0]
	x_raw := b[1:4]
	y_raw := b[4:7]
	offset_raw := b[7:13]
	length_raw := b[13:17]
	x := readUint24(x_raw)
	y := readUint24(y_raw)
	offset := readUint48(offset_raw)
	length := uint64(binary.LittleEndian.Uint32(length_raw))
	if z_raw&0b10000000 == 0 {
		return 0, Zxy{Z: uint8(z_raw), X: uint32(x), Y: uint32(y)}, Range{Offset: offset, Length: length}
	} else {
		leaf_z := z_raw & 0b01111111
		return leaf_z, Zxy{Z: leaf_z, X: uint32(x), Y: uint32(y)}, Range{Offset: offset, Length: length}
	}
}

func ParseDirectoryV2(dir_bytes []byte) DirectoryV2 {
	the_dir := DirectoryV2{Entries: make(map[Zxy]Range), Leaves: make(map[Zxy]Range)}
	var maxz uint8
	for i := 0; i < len(dir_bytes)/17; i++ {
		leaf_z, zxy, rng := ParseEntryV2(dir_bytes[i*17 : i*17+17])
		if leaf_z == 0 {
			the_dir.Entries[zxy] = rng
		} else {
			maxz = leaf_z // todo check spec
			the_dir.Leaves[zxy] = rng
		}
	}
	the_dir.LeafZ = maxz
	return the_dir
}

func ParseHeaderV2(reader io.Reader) ([]byte, DirectoryV2) {
	magic_num := make([]byte, 2)
	io.ReadFull(reader, magic_num)
	version := make([]byte, 2)
	io.ReadFull(reader, version)
	metadata_len_bytes := make([]byte, 4)
	io.ReadFull(reader, metadata_len_bytes)
	metadata_len := binary.LittleEndian.Uint32(metadata_len_bytes)
	rootdir_len_bytes := make([]byte, 2)
	io.ReadFull(reader, rootdir_len_bytes)
	rootdir_len := int(binary.LittleEndian.Uint16(rootdir_len_bytes))
	metadata_bytes := make([]byte, metadata_len)
	io.ReadFull(reader, metadata_bytes)
	dir_bytes := make([]byte, rootdir_len*17)
	io.ReadFull(reader, dir_bytes)
	the_dir := ParseDirectoryV2(dir_bytes)
	return metadata_bytes, the_dir
}
