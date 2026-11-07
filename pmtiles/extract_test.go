package pmtiles

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
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

// readTileContents returns the contents of every tile in a local archive, by tile ID.
func readTileContents(t *testing.T, path string) map[uint64][]byte {
	archive, err := os.ReadFile(path)
	assert.Nil(t, err)
	header, err := DeserializeHeader(archive[0:HeaderV3LenBytes])
	assert.Nil(t, err)

	contents := make(map[uint64][]byte)
	var readDirectory func(offset uint64, length uint64)
	readDirectory = func(offset uint64, length uint64) {
		entries := DeserializeEntries(bytes.NewBuffer(archive[offset:offset+length]), header.InternalCompression)
		for _, entry := range entries {
			if entry.RunLength == 0 {
				readDirectory(header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length))
				continue
			}
			start := header.TileDataOffset + entry.Offset
			for i := uint64(0); i < uint64(entry.RunLength); i++ {
				contents[entry.TileID+i] = archive[start : start+uint64(entry.Length)]
			}
		}
	}
	readDirectory(header.RootOffset, header.RootLength)
	return contents
}

func TestExtractMaxZoomFromLocalArchive(t *testing.T) {
	for _, leaves := range []bool{false, true} {
		t.Run(fmt.Sprintf("leaves=%v", leaves), func(t *testing.T) {
			tiles := make(map[Zxy][]byte)
			for z := uint8(0); z <= 2; z++ {
				for x := uint32(0); x < 1<<z; x++ {
					for y := uint32(0); y < 1<<z; y++ {
						tiles[Zxy{z, x, y}] = []byte(fmt.Sprintf("tile %d/%d/%d", z, x, y))
					}
				}
			}
			header := HeaderV3{
				TileType:  Mvt,
				Clustered: true,
				MinLonE7:  -180 * 10000000,
				MinLatE7:  -85 * 10000000,
				MaxLonE7:  180 * 10000000,
				MaxLatE7:  85 * 10000000,
			}
			dir := t.TempDir()
			input := filepath.Join(dir, "input.pmtiles")
			output := filepath.Join(dir, "output.pmtiles")
			assert.Nil(t, os.WriteFile(input, fakeArchive(header, map[string]interface{}{}, tiles, leaves, Gzip), 0o600))

			logger := log.New(io.Discard, "", 0)
			err := Extract(context.Background(), logger, "", input, -1, 1, "", "", output, 4, 0.05, false)
			assert.Nil(t, err)
			assert.Nil(t, Verify(logger, output))

			expected := make(map[uint64][]byte)
			for zxy, content := range tiles {
				if zxy.Z <= 1 {
					expected[ZxyToID(zxy.Z, zxy.X, zxy.Y)] = content
				}
			}
			assert.Equal(t, expected, readTileContents(t, output))
		})
	}
}

// archiveServer serves an archive over HTTP with range requests, ETags and If-Match,
// through http.ServeContent. replaceAfterRequests > 0 replaces the archive's ETag after that many requests.
type archiveServer struct {
	mu                   sync.Mutex
	archive              []byte
	etag                 string
	requests             int
	replaceAfterRequests int
}

func (s *archiveServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	if s.replaceAfterRequests > 0 && s.requests > s.replaceAfterRequests {
		s.etag = `"replaced"`
	}
	etag := s.etag
	s.mu.Unlock()
	w.Header().Set("ETag", etag)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(s.archive))
}

func extractTestTiles() map[Zxy][]byte {
	tiles := make(map[Zxy][]byte)
	for z := uint8(0); z <= 2; z++ {
		for x := uint32(0); x < 1<<z; x++ {
			for y := uint32(0); y < 1<<z; y++ {
				tiles[Zxy{z, x, y}] = []byte(fmt.Sprintf("tile %d/%d/%d", z, x, y))
			}
		}
	}
	return tiles
}

func extractTestHeader() HeaderV3 {
	return HeaderV3{
		TileType:  Mvt,
		Clustered: true,
		MinLonE7:  -180 * 10000000,
		MinLatE7:  -85 * 10000000,
		MaxLonE7:  180 * 10000000,
		MaxLatE7:  85 * 10000000,
	}
}

func TestExtractFailsWhenTheSourceChanges(t *testing.T) {
	server := &archiveServer{
		archive:              fakeArchive(extractTestHeader(), map[string]interface{}{}, extractTestTiles(), true, Gzip),
		etag:                 `"original"`,
		replaceAfterRequests: 1,
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	output := filepath.Join(t.TempDir(), "output.pmtiles")
	err := Extract(context.Background(), log.New(io.Discard, "", 0), "", httpServer.URL+"/input.pmtiles", -1, 1, "", "", output, 4, 0.05, false)

	var refreshRequired *RefreshRequiredError
	assert.True(t, errors.As(err, &refreshRequired), "expected a RefreshRequiredError, got %v", err)
}
