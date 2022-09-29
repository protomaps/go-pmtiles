package pmtiles

import (
	"testing"
)

func TestDirectoryRoundtrip(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})
	entries = append(entries, EntryV3{1, 1, 1, 1})
	entries = append(entries, EntryV3{2, 2, 2, 2})

	serialized := serialize_entries(entries)
	result := deserialize_entries(serialized)
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
	header.MinLon = 1.1
	header.MinLat = 2.1
	header.MaxLon = 1.2
	header.MaxLat = 2.2
	header.CenterZoom = 3
	header.CenterLon = 3.1
	header.CenterLat = 3.2
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
	if result.MinLon != 1.1 {
		t.Fatalf(`expected to be 1.1`)
	}
	if result.MinLat != 2.1 {
		t.Fatalf(`expected to be 2.1`)
	}
	if result.MaxLon != 1.2 {
		t.Fatalf(`expected to be 1.2`)
	}
	if result.MaxLat != 2.2 {
		t.Fatalf(`expected to be 2.2`)
	}
	if result.CenterZoom != 3 {
		t.Fatalf(`expected to be 3`)
	}
	if result.CenterLon != 3.1 {
		t.Fatalf(`expected to be 3.1`)
	}
	if result.CenterLat != 3.2 {
		t.Fatalf(`expected to be 3.2`)
	}
}
