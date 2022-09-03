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
