package pmtiles

import (
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"testing"
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

	reencoded, result, datalen, addressed, contents := ReencodeEntries(entries)

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

	reencoded, result, datalen, addressed, contents := ReencodeEntries(entries)

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

	_, result, _, _, _ := ReencodeEntries(entries)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(30))
}

func TestMergeRanges(t *testing.T) {
	ranges := make([]SrcDstRange, 0)
	ranges = append(ranges, SrcDstRange{0, 0, 50})
	ranges = append(ranges, SrcDstRange{60, 60, 60})

	result := MergeRanges(ranges, 0.1)

	assert.Equal(t, 1, len(result))
	assert.Equal(t, SrcDstRange{0, 0, 120}, result[0].Rng)
	assert.Equal(t, 2, len(result[0].CopyDiscards))
	assert.Equal(t, CopyDiscard{50, 10}, result[0].CopyDiscards[0])
	assert.Equal(t, CopyDiscard{60, 0}, result[0].CopyDiscards[1])
}

func TestMergeRangesMultiple(t *testing.T) {
	ranges := make([]SrcDstRange, 0)
	ranges = append(ranges, SrcDstRange{0, 0, 50})
	ranges = append(ranges, SrcDstRange{60, 60, 10})
	ranges = append(ranges, SrcDstRange{80, 80, 10})

	result := MergeRanges(ranges, 0.3)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, SrcDstRange{0, 0, 90}, result[0].Rng)
	assert.Equal(t, 3, len(result[0].CopyDiscards))
	fmt.Println(result)
}
