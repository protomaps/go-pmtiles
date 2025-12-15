package pmtiles

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
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

func TestRemapMergeEntries(t *testing.T) {
	entries := make([]MergeEntry, 0)
	entries = append(entries, MergeEntry{EntryV3{0, 0, 100, 1}, 0, 0})
	entries = append(entries, MergeEntry{EntryV3{1, 0, 100, 2}, 1, 0})
	remapped, addressedTiles, tileContents, length, _ := remapMergeEntries(entries, 2)
	assert.Equal(t, uint64(200), length)
	assert.Equal(t, uint64(3), addressedTiles)
	assert.Equal(t, uint64(2), tileContents)
	assert.Equal(t, uint64(0), remapped[0].Entry.Offset)
	assert.Equal(t, uint64(0), remapped[0].InputOffset)
	assert.Equal(t, uint64(100), remapped[1].Entry.Offset)
	assert.Equal(t, uint64(0), remapped[1].InputOffset)
}

func TestRemapMergeEntriesBackreference(t *testing.T) {
	entries := make([]MergeEntry, 0)
	entries = append(entries, MergeEntry{EntryV3{0, 0, 100, 1}, 0, 0})
	entries = append(entries, MergeEntry{EntryV3{1, 0, 100, 1}, 1, 0})
	entries = append(entries, MergeEntry{EntryV3{2, 100, 100, 1}, 0, 100})
	entries = append(entries, MergeEntry{EntryV3{3, 100, 100, 1}, 1, 100})
	entries = append(entries, MergeEntry{EntryV3{4, 0, 100, 1}, 1, 0}) // the backreference
	remapped, addressedTiles, tileContents, length, _ := remapMergeEntries(entries, 2)
	assert.Equal(t, uint64(400), length)
	assert.Equal(t, uint64(5), addressedTiles)
	assert.Equal(t, uint64(4), tileContents)
	assert.Equal(t, uint64(100), remapped[4].Entry.Offset)
	assert.Equal(t, uint64(0), remapped[4].InputOffset)
}

func TestValidateArchiveClustered(t *testing.T) {
	var h1 HeaderV3
	h1.Clustered = true
	archive1 := fakeArchive(h1, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	var h2 HeaderV3
	archive2 := fakeArchive(h2, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	_, _, err, errIdx := prepareInputs([]io.ReadSeeker{bytes.NewReader(archive1), bytes.NewReader(archive2)})
	assert.ErrorContains(t, err, "must be clustered")
	assert.Equal(t, 1, errIdx)
}

func TestValidateArchiveTileType(t *testing.T) {
	var h1 HeaderV3
	h1.Clustered = true
	h1.TileType = Jpeg
	archive1 := fakeArchive(h1, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3}}, false, Gzip)
	var h2 HeaderV3
	h2.Clustered = true
	h2.TileType = Png
	archive2 := fakeArchive(h2, map[string]interface{}{}, map[Zxy][]byte{
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	_, _, err, errIdx := prepareInputs([]io.ReadSeeker{bytes.NewReader(archive1), bytes.NewReader(archive2)})
	assert.ErrorContains(t, err, "png does not match jpg")
	assert.Equal(t, 1, errIdx)
}

func TestValidateArchiveTileCompression(t *testing.T) {
	var h1 HeaderV3
	h1.Clustered = true
	h1.TileType = UnknownTileType
	h1.TileCompression = Gzip
	archive1 := fakeArchive(h1, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3}}, false, Gzip)
	var h2 HeaderV3
	h2.Clustered = true
	h2.TileType = UnknownTileType
	h2.TileCompression = Brotli
	archive2 := fakeArchive(h2, map[string]interface{}{}, map[Zxy][]byte{
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	_, _, err, errIdx := prepareInputs([]io.ReadSeeker{bytes.NewReader(archive1), bytes.NewReader(archive2)})
	assert.ErrorContains(t, err, "br does not match gzip")
	assert.Equal(t, 1, errIdx)
}

func TestValidateArchiveInternalCompression(t *testing.T) {
	var h1 HeaderV3
	h1.Clustered = true
	h1.TileType = UnknownTileType
	archive1 := fakeArchive(h1, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3}}, false, NoCompression)
	var h2 HeaderV3
	h2.Clustered = true
	h2.TileType = UnknownTileType
	archive2 := fakeArchive(h2, map[string]interface{}{}, map[Zxy][]byte{
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	_, _, err, errIdx := prepareInputs([]io.ReadSeeker{bytes.NewReader(archive1), bytes.NewReader(archive2)})
	assert.ErrorContains(t, err, "gzip does not match none")
	assert.Equal(t, 1, errIdx)
}

func TestValidateSuccess(t *testing.T) {
	var h1 HeaderV3
	h1.Clustered = true
	archive1 := fakeArchive(h1, map[string]interface{}{}, map[Zxy][]byte{
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	var h2 HeaderV3
	h2.Clustered = true
	archive2 := fakeArchive(h2, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3}}, false, Gzip)
	_, mergeEntries, err, _ := prepareInputs([]io.ReadSeeker{bytes.NewReader(archive1), bytes.NewReader(archive2)})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mergeEntries))
	assert.Equal(t, uint64(0), mergeEntries[0].Entry.TileID)
}

func TestValidateDisjoint(t *testing.T) {
	var h1 HeaderV3
	h1.Clustered = true
	archive1 := fakeArchive(h1, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3}}, false, Gzip)
	var h2 HeaderV3
	h2.Clustered = true
	archive2 := fakeArchive(h2, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3}}, false, Gzip)
	_, _, err, errIdx := prepareInputs([]io.ReadSeeker{bytes.NewReader(archive1), bytes.NewReader(archive2)})
	assert.ErrorContains(t, err, "1 overlapping tiles, starting with 0 0 0")
	assert.Equal(t, 1, errIdx)
}
