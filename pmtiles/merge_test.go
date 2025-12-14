package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchMergeEntriesDifferentInputs(t *testing.T) {
	entries := make([]MergeEntry, 0)
	entries = append(entries, MergeEntry{EntryV3{0, 100, 100, 1}, 0, 100})
	entries = append(entries, MergeEntry{EntryV3{1, 200, 100, 1}, 1, 200})
	result := batchMergeEntries(entries, 2)
	assert.Equal(t, 2, len(result))
}

func TestBatchMergeEntries(t *testing.T) {
	entries := make([]MergeEntry, 0)
	entries = append(entries, MergeEntry{EntryV3{0, 0, 100, 1}, 0, 0})
	entries = append(entries, MergeEntry{EntryV3{1, 100, 100, 1}, 0, 100})
	result := batchMergeEntries(entries, 1)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 200, int(result[0].Length))
}

func TestBatchMergeBackreference(t *testing.T) {
	entries := make([]MergeEntry, 0)
	entries = append(entries, MergeEntry{EntryV3{0, 0, 100, 1}, 0, 0})
	entries = append(entries, MergeEntry{EntryV3{1, 100, 100, 1}, 0, 100})
	entries = append(entries, MergeEntry{EntryV3{2, 0, 100, 1}, 0, 0})
	result := batchMergeEntries(entries, 1)
	assert.Equal(t, 1, len(result))
}

func TestZoomBounds(t *testing.T) {
	entries := make([]MergeEntry, 0)
	entries = append(entries, MergeEntry{EntryV3{0, 0, 100, 1}, 0, 0})
	entries = append(entries, MergeEntry{EntryV3{3, 100, 100, 1}, 0, 100})
	minZoom, maxZoom := zoomBounds(entries)
	assert.Equal(t, uint8(0), minZoom)
	assert.Equal(t, uint8(1), maxZoom)
	entries = append(entries, MergeEntry{EntryV3{4, 100, 100, 2}, 0, 100})
	_, maxZoom = zoomBounds(entries)
	assert.Equal(t, uint8(2), maxZoom)
}

func TestBounds(t *testing.T) {
	var h1 HeaderV3
	var h2 HeaderV3
	h1.MinLonE7 = -1
	h1.MinLatE7 = -2
	h1.MaxLonE7 = 1
	h1.MaxLatE7 = 2
	h2.MinLonE7 = -2
	h2.MinLatE7 = -1
	h2.MaxLonE7 = 2
	h2.MaxLatE7 = 1
	minLon, minLat, maxLon, maxLat := bounds([]HeaderV3{h1, h2})
	assert.Equal(t, int32(-2), minLon)
	assert.Equal(t, int32(-2), minLat)
	assert.Equal(t, int32(2), maxLon)
	assert.Equal(t, int32(2), maxLat)
}
