package pmtiles

import (
	"bytes"
	"math/rand"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestDirectoryRoundtrip(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})
	entries = append(entries, EntryV3{1, 1, 1, 1})
	entries = append(entries, EntryV3{2, 2, 2, 2})

	serialized := serialize_entries(entries)
	result := deserialize_entries(bytes.NewBuffer(serialized))
	if len(result) != 3 {
		t.Fatalf(`expected %d to be 3`, len(result))
	}
	if result[0].TileId != 0 {
		t.Fatalf(`expected to be 0`)
	}
	if result[0].Offset != 0 {
		t.Fatalf(`expected to be 0`)
	}
	if result[0].Length != 0 {
		t.Fatalf(`expected to be 0`)
	}
	if result[0].RunLength != 0 {
		t.Fatalf(`expected to be 0`)
	}
	if result[1].TileId != 1 {
		t.Fatalf(`expected to be 1`)
	}
	if result[1].Offset != 1 {
		t.Fatalf(`expected to be 1`)
	}
	if result[1].Length != 1 {
		t.Fatalf(`expected to be 1`)
	}
	if result[1].RunLength != 1 {
		t.Fatalf(`expected to be 1`)
	}
	if result[2].TileId != 2 {
		t.Fatalf(`expected to be 2`)
	}
	if result[2].Offset != 2 {
		t.Fatalf(`expected to be 2`)
	}
	if result[2].Length != 2 {
		t.Fatalf(`expected to be 2`)
	}
	if result[2].RunLength != 2 {
		t.Fatalf(`expected to be 2`)
	}
}

func TestHeaderRoundtrip(t *testing.T) {
	header := HeaderV3{}
	header.RootOffset = 1
	header.RootLength = 2
	header.MetadataOffset = 3
	header.MetadataLength = 4
	header.LeafDirectoryOffset = 5
	header.LeafDirectoryLength = 6
	header.TileDataOffset = 7
	header.TileDataLength = 8
	header.AddressedTilesCount = 9
	header.TileEntriesCount = 10
	header.TileContentsCount = 11
	header.Clustered = true
	header.InternalCompression = Gzip
	header.TileCompression = Brotli
	header.TileType = Mvt
	header.MinZoom = 1
	header.MaxZoom = 2
	header.MinLonE7 = 1.1 * 10000000
	header.MinLatE7 = 2.1 * 10000000
	header.MaxLonE7 = 1.2 * 10000000
	header.MaxLatE7 = 2.2 * 10000000
	header.CenterZoom = 3
	header.CenterLonE7 = 3.1 * 10000000
	header.CenterLatE7 = 3.2 * 10000000
	b := serialize_header(header)
	result := deserialize_header(b)
	if result.RootOffset != 1 {
		t.Fatalf(`expected to be 1`)
	}
	if result.RootLength != 2 {
		t.Fatalf(`expected to be 2`)
	}
	if result.MetadataOffset != 3 {
		t.Fatalf(`expected to be 3`)
	}
	if result.MetadataLength != 4 {
		t.Fatalf(`expected to be 4`)
	}
	if result.LeafDirectoryOffset != 5 {
		t.Fatalf(`expected to be 5`)
	}
	if result.LeafDirectoryLength != 6 {
		t.Fatalf(`expected to be 6`)
	}
	if result.TileDataOffset != 7 {
		t.Fatalf(`expected to be 7`)
	}
	if result.TileDataLength != 8 {
		t.Fatalf(`expected to be 8`)
	}
	if result.AddressedTilesCount != 9 {
		t.Fatalf(`expected to be 9`)
	}
	if result.TileEntriesCount != 10 {
		t.Fatalf(`expected to be 10`)
	}
	if result.TileContentsCount != 11 {
		t.Fatalf(`expected to be 11`)
	}
	if !result.Clustered {
		t.Fatalf(`expected to be true`)
	}
	if result.InternalCompression != Gzip {
		t.Fatalf(`expected to be gzip`)
	}
	if result.TileCompression != Brotli {
		t.Fatalf(`expected to be brotli`)
	}
	if result.TileType != Mvt {
		t.Fatalf(`expected to be mvt`)
	}
	if result.MinZoom != 1 {
		t.Fatalf(`expected to be 1`)
	}
	if result.MaxZoom != 2 {
		t.Fatalf(`expected to be 2`)
	}
	if result.MinLonE7 != 11000000 {
		t.Fatalf(`expected to be 1.1`)
	}
	if result.MinLatE7 != 21000000 {
		t.Fatalf(`expected to be 2.1`)
	}
	if result.MaxLonE7 != 12000000 {
		t.Fatalf(`expected to be 1.2`)
	}
	if result.MaxLatE7 != 22000000 {
		t.Fatalf(`expected to be 2.2`)
	}
	if result.CenterZoom != 3 {
		t.Fatalf(`expected to be 3`)
	}
	if result.CenterLonE7 != 31000000 {
		t.Fatalf(`expected to be 3.1`)
	}
	if result.CenterLatE7 != 32000000 {
		t.Fatalf(`expected to be 3.2`)
	}
}

func TestOptimizeDirectories(t *testing.T) {
	rand.Seed(3857)
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 100, 1})
	_, leaves_bytes, num_leaves := optimize_directories(entries, 100)
	if len(leaves_bytes) > 0 || num_leaves != 0 {
		t.Fatalf("leaves bytes should be empty")
	}

	entries = make([]EntryV3, 0)
	var i uint64
	var offset uint64
	for ; i < 1000; i++ {
		randtilesize := rand.Intn(1000000)
		entries = append(entries, EntryV3{i, offset, uint32(randtilesize), 1})
		offset += uint64(randtilesize)
	}

	root_bytes, leaves_bytes, num_leaves := optimize_directories(entries, 1024)

	if len(root_bytes) > 1024 {
		t.Fatalf("root bytes")
	}

	if num_leaves == 0 || len(leaves_bytes) == 0 {
		t.Fatalf("expected leaves")
	}
}

func TestFindTileMissing(t *testing.T) {
	entries := make([]EntryV3,0)
	_, ok := find_tile(entries,0)
	if ok {
		t.Fatalf("Expected not ok")
	}
}

func TestFindTileFirstEntry(t *testing.T) {
	entries := []EntryV3{{TileId:100, Offset: 1, Length: 1, RunLength:1}}
	entry, ok := find_tile(entries,100)
	assert.Equal(t,true,ok)
	assert.Equal(t,uint64(1),entry.Offset)
	assert.Equal(t,uint32(1),entry.Length)
	_, ok = find_tile(entries,101)
	assert.Equal(t,false,ok)
}

func TestFindTileMultipleEntries(t *testing.T) {
	entries := []EntryV3{
		{TileId:100, Offset: 1, Length: 1, RunLength:2},
	}
	entry, ok := find_tile(entries,101)
	assert.Equal(t,true,ok)
	assert.Equal(t,uint64(1),entry.Offset)
	assert.Equal(t,uint32(1),entry.Length)

	entries = []EntryV3{
		{TileId:100, Offset: 1, Length: 1, RunLength:1},
		{TileId:150, Offset: 2, Length: 2, RunLength:2},
	}
	entry, ok = find_tile(entries,151)
	assert.Equal(t,true,ok)
	assert.Equal(t,uint64(2),entry.Offset)
	assert.Equal(t,uint32(2),entry.Length)

	entries = []EntryV3{
		{TileId:50, Offset: 1, Length: 1, RunLength:2},
		{TileId:100, Offset: 2, Length: 2, RunLength:1},
		{TileId:150, Offset: 3, Length: 3, RunLength:1},
	}
	entry, ok = find_tile(entries,51)
	assert.Equal(t,true,ok)
	assert.Equal(t,uint64(1),entry.Offset)
	assert.Equal(t,uint32(1),entry.Length)
}

func TestFindTileLeafSearch(t *testing.T) {
	entries := []EntryV3{
		{TileId:100, Offset: 1, Length: 1, RunLength:0},
	}
	entry, ok := find_tile(entries,150)
	assert.Equal(t,true,ok)
	assert.Equal(t,uint64(1),entry.Offset)
	assert.Equal(t,uint32(1),entry.Length)
}
