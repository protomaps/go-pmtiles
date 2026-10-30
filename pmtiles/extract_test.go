package pmtiles

import (
	"bytes"
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRelevantEntries(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 1})

	bitmap := roaring64.New()
	bitmap.Add(0)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 1)
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesRunLength(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 5})

	bitmap := roaring64.New()
	bitmap.Add(1)
	bitmap.Add(2)
	bitmap.Add(4)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 2)
	assert.Equal(t, tiles[0].RunLength, uint32(2))
	assert.Equal(t, tiles[1].RunLength, uint32(1))
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesLeaf(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(1)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 0)
	assert.Equal(t, len(leaves), 1)
}

func TestRelevantEntriesNotLeaf(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})
	entries = append(entries, EntryV3{2, 0, 0, 1})
	entries = append(entries, EntryV3{4, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(3)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 0)
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesMaxZoom(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(6)
	_, leaves := RelevantEntries(bitmap, 1, entries)
	assert.Equal(t, len(leaves), 0)

	_, leaves = RelevantEntries(bitmap, 2, entries)
	assert.Equal(t, len(leaves), 1)
}

func TestReencodeEntries(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 1})
	entries = append(entries, EntryV3{1, 500, 20, 2})

	reencoded, result, datalen, addressed, contents := reencodeEntries(entries)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(10))
	assert.Equal(t, result[1].SrcOffset, uint64(500))
	assert.Equal(t, result[1].Length, uint64(20))

	assert.Equal(t, 2, len(reencoded))
	assert.Equal(t, reencoded[0].Offset, uint64(0))
	assert.Equal(t, reencoded[1].Offset, uint64(10))

	assert.Equal(t, uint64(30), datalen)
	assert.Equal(t, uint64(3), addressed)
	assert.Equal(t, uint64(2), contents)
}

func TestReencodeEntriesDuplicate(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 1})
	entries = append(entries, EntryV3{1, 500, 20, 1})
	entries = append(entries, EntryV3{2, 400, 10, 1})

	reencoded, result, datalen, addressed, contents := reencodeEntries(entries)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(10))
	assert.Equal(t, result[1].SrcOffset, uint64(500))
	assert.Equal(t, result[1].Length, uint64(20))

	assert.Equal(t, len(reencoded), 3)
	assert.Equal(t, reencoded[0].Offset, uint64(0))
	assert.Equal(t, reencoded[1].Offset, uint64(10))
	assert.Equal(t, reencoded[2].Offset, uint64(0))

	assert.Equal(t, uint64(30), datalen)
	assert.Equal(t, uint64(3), addressed)
	assert.Equal(t, uint64(2), contents)
}

func TestReencodeContiguous(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 0})
	entries = append(entries, EntryV3{1, 410, 20, 0})

	_, result, _, _, _ := reencodeEntries(entries)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(30))
}

func TestMergeRanges(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 60})

	result, totalTransferBytes := mergeRanges(ranges, 0.1)

	assert.Equal(t, 1, result.Len())
	assert.Equal(t, uint64(120), totalTransferBytes)
	front := result.Front().Value.(overfetchRange)
	assert.Equal(t, srcDstRange{0, 0, 120}, front.Rng)
	assert.Equal(t, 2, len(front.CopyDiscards))
	assert.Equal(t, copyDiscard{50, 10}, front.CopyDiscards[0])
	assert.Equal(t, copyDiscard{60, 0}, front.CopyDiscards[1])
}

func TestMergeRangesMultiple(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 10})
	ranges = append(ranges, srcDstRange{80, 80, 10})

	result, totalTransferBytes := mergeRanges(ranges, 0.3)
	front := result.Front().Value.(overfetchRange)
	assert.Equal(t, uint64(90), totalTransferBytes)
	assert.Equal(t, 1, result.Len())
	assert.Equal(t, srcDstRange{0, 0, 90}, front.Rng)
	assert.Equal(t, 3, len(front.CopyDiscards))
}

func TestMergeRangesNonSrcOrdered(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{20, 0, 50})
	ranges = append(ranges, srcDstRange{0, 60, 50})

	result, _ := mergeRanges(ranges, 0.1)
	assert.Equal(t, 2, result.Len())
}

type section func(header HeaderV3) (uint64, uint64)

func rootSection(h HeaderV3) (uint64, uint64)     { return h.RootOffset, h.RootLength }
func metadataSection(h HeaderV3) (uint64, uint64) { return h.MetadataOffset, h.MetadataLength }
func leafSection(h HeaderV3) (uint64, uint64)     { return h.LeafDirectoryOffset, h.LeafDirectoryLength }
func tileSection(h HeaderV3) (uint64, uint64)     { return h.TileDataOffset, h.TileDataLength }

func replacingSource(t *testing.T, v1, v2 []byte, sec section) (*httptest.Server, *bool) {
	header, err := DeserializeHeader(v1[0:HeaderV3LenBytes])
	assert.Nil(t, err)
	offset, length := sec(header)
	assert.Greater(t, length, uint64(0))
	var mu sync.Mutex
	replaced := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start uint64
		fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start)
		mu.Lock()
		if !replaced && start >= offset && start < offset+length {
			replaced = true
		}
		data := v1
		if replaced {
			data = v2
		}
		mu.Unlock()
		w.Header().Set("ETag", generateEtag(data))
		http.ServeContent(w, r, "archive.pmtiles", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(server.Close)
	return server, &replaced
}

func extractReplaced(t *testing.T, v1, v2 []byte, sec section) {
	server, replaced := replacingSource(t, v1, v2, sec)
	output := filepath.Join(t.TempDir(), "output.pmtiles")
	err := Extract(context.Background(), log.New(io.Discard, "", 0), server.URL, "archive.pmtiles", -1, -1, "", "", output, 1, 0.05, false)
	assert.True(t, *replaced)
	assert.True(t, isRefreshRequiredError(err), "expected RefreshRequiredError, got %v", err)
}

func TestExtractSourceReplacedBeforeTileRead(t *testing.T) {
	header := HeaderV3{TileType: Mvt, Clustered: true}
	v1 := fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
	}, false, Gzip)
	v2 := fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {4, 5, 6, 7},
	}, false, Gzip)
	extractReplaced(t, v1, v2, tileSection)
}

func TestExtractSourceReplacedBeforeLeafRead(t *testing.T) {
	header := HeaderV3{TileType: Mvt, Clustered: true}
	v1 := fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, true, Gzip)
	v2 := fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {4, 5},
		{1, 1, 1}: {6, 7},
	}, false, Gzip)
	extractReplaced(t, v1, v2, leafSection)
}

func TestExtractSourceReplacedBeforeRootRead(t *testing.T) {
	v1 := fakeArchive(HeaderV3{TileType: Mvt, Clustered: true}, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	v2 := fakeArchive(HeaderV3{TileType: Mvt, Clustered: true, CenterZoom: 4}, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	extractReplaced(t, v1, v2, rootSection)
}

func TestExtractSourceReplacedBeforeMetadataRead(t *testing.T) {
	header := HeaderV3{TileType: Mvt, Clustered: true}
	v1 := fakeArchive(header, map[string]interface{}{
		"meta": "data",
	}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	v2 := fakeArchive(header, map[string]interface{}{
		"meta": "data2",
	}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	extractReplaced(t, v1, v2, metadataSection)
}
