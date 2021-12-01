package pmtiles

import (
	"bufio"
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
	Length uint32
}

type Directory struct {
	Entries map[Zxy]Range
	LeafZ   uint8
	Leaves  map[Zxy]Range
}

func (d Directory) SizeBytes() int {
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

func ParseDirectory(reader io.Reader, numEntries uint32) Directory {
	z_raw := make([]byte, 1)
	x_raw := make([]byte, 3)
	y_raw := make([]byte, 3)
	offset_raw := make([]byte, 6)
	length_raw := make([]byte, 4)

	the_dir := Directory{Entries: make(map[Zxy]Range), Leaves: make(map[Zxy]Range)}
	var maxz uint8
	for i := 0; i < int(numEntries); i++ {
		io.ReadFull(reader, z_raw)
		io.ReadFull(reader, x_raw)
		io.ReadFull(reader, y_raw)
		io.ReadFull(reader, offset_raw)
		io.ReadFull(reader, length_raw)
		x := readUint24(x_raw)
		y := readUint24(y_raw)
		offset := readUint48(offset_raw)
		length := binary.LittleEndian.Uint32(length_raw)

		z := z_raw[0]
		if z&0b10000000 == 0 {
			the_dir.Entries[Zxy{Z: uint8(z), X: uint32(x), Y: uint32(y)}] = Range{Offset: offset, Length: length}
		} else {
			leaf_z := z & 0b01111111
			maxz = leaf_z
			if leaf_z != maxz {
				// raise an error, out of spec pmtiles
			}
			the_dir.Leaves[Zxy{Z: leaf_z, X: uint32(x), Y: uint32(y)}] = Range{Offset: offset, Length: length}
		}
	}
	the_dir.LeafZ = maxz
	return the_dir
}

func ParseHeader(reader io.Reader) ([]byte, Directory) {
	magic_num := make([]byte, 2)
	io.ReadFull(reader, magic_num)
	version := make([]byte, 2)
	io.ReadFull(reader, version)
	metadata_len_bytes := make([]byte, 4)
	io.ReadFull(reader, metadata_len_bytes)
	metadata_len := binary.LittleEndian.Uint32(metadata_len_bytes)
	rootdir_len_bytes := make([]byte, 2)
	io.ReadFull(reader, rootdir_len_bytes)
	rootdir_len := binary.LittleEndian.Uint16(rootdir_len_bytes)
	metadata_bytes := make([]byte, metadata_len)
	io.ReadFull(reader, metadata_bytes)
	the_dir := ParseDirectory(bufio.NewReader(reader), uint32(rootdir_len))
	return metadata_bytes, the_dir
}
