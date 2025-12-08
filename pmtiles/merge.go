package pmtiles

import (
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"io"
	"log"
	"math"
	"os"
	"sort"
)

type MergeEntry struct {
	Entry       EntryV3
	InputIdx    int    // the index of the input archive 0...N
	InputOffset uint64 // the original offset of the entry in the archive's tile section
}

type MergeOp struct {
	InputIdx    int
	InputOffset uint64
	Length      uint64
}

func Merge(logger *log.Logger, inputs []string) error {
	union := roaring64.New()
	var mergedEntries []MergeEntry

	minLonE7 := int32(math.MaxInt32)
	minLatE7 := int32(math.MaxInt32)
	maxLonE7 := int32(math.MinInt32)
	maxLatE7 := int32(math.MinInt32)

	var handles []*os.File
	var headers []HeaderV3

	for archiveIdx, archive := range inputs[:len(inputs)-1] {
		f, _ := os.OpenFile(archive, os.O_RDONLY, 0666)
		handles = append(handles, f)

		buf := make([]byte, HeaderV3LenBytes)
		_, _ = f.Read(buf)
		h, _ := DeserializeHeader(buf)
		headers = append(headers, h)

		if !h.Clustered {
			return fmt.Errorf("Archive must be clustered")
		}

		if archiveIdx > 0 {
			if h.TileType != headers[0].TileType {
				return fmt.Errorf("Tile types do not match")
			}
			if h.TileCompression != headers[0].TileCompression {
				return fmt.Errorf("Tile compressions do not match")
			}
			if h.InternalCompression != headers[0].InternalCompression {
				return fmt.Errorf("Internal compressions do not match")
			}
		}

		if h.MinLonE7 < minLonE7 {
			minLonE7 = h.MinLonE7
		}
		if h.MinLatE7 < minLatE7 {
			minLatE7 = h.MinLatE7
		}
		if h.MaxLonE7 > maxLonE7 {
			maxLonE7 = h.MaxLonE7
		}
		if h.MaxLatE7 > maxLatE7 {
			maxLatE7 = h.MaxLatE7
		}

		tileset := roaring64.New()
		_ = IterateEntries(h,
			func(offset uint64, length uint64) ([]byte, error) {
				return io.ReadAll(io.NewSectionReader(f, int64(offset), int64(length)))
			},
			func(e EntryV3) {
				tileset.AddRange(e.TileID, e.TileID+uint64(e.RunLength))
				mergedEntries = append(mergedEntries, MergeEntry{Entry: e, InputOffset: e.Offset, InputIdx: archiveIdx})
			})

		if union.Intersects(tileset) {
			return fmt.Errorf("Tilesets intersect")
		}
		union.Or(tileset)
	}

	// sort all MergeEntries
	sort.Slice(mergedEntries, func(i, j int) bool {
		return mergedEntries[i].Entry.TileID < mergedEntries[j].Entry.TileID
	})

	// renumber the offsets
	acc := uint64(0)
	addressedTiles := uint64(0)
	tileContents := roaring64.New()
	for idx := range mergedEntries {
		// TODO: this algo is broken with any deduplication of tiles
		// need to bookkeep on the max seen offset in each input archive
		mergedEntries[idx].Entry.Offset = acc
		acc += uint64(mergedEntries[idx].Entry.Length)
		addressedTiles += uint64(mergedEntries[idx].Entry.RunLength)
		tileContents.Add(mergedEntries[idx].Entry.Offset)
	}

	// construct a directory
	tmp := make([]EntryV3, len(mergedEntries))
	for i := range mergedEntries {
		tmp[i] = mergedEntries[i].Entry
	}

	rootBytes, leavesBytes, _ := optimizeDirectories(tmp, 16384-HeaderV3LenBytes, Gzip)

	var header HeaderV3

	header.RootOffset = HeaderV3LenBytes
	header.RootLength = uint64(len(rootBytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.MetadataLength = headers[0].MetadataLength
	header.InternalCompression = headers[0].InternalCompression
	header.TileCompression = headers[0].TileCompression
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(leavesBytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength

	header.MinLonE7 = minLonE7
	header.MinLatE7 = minLatE7
	header.MaxLonE7 = maxLonE7
	header.MaxLatE7 = maxLatE7

	// although we can rely on the input header data,
	// it's cheap and more reliable to re-calculate these from scratch
	firstZ, _, _ := IDToZxy(mergedEntries[0].Entry.TileID)
	header.MinZoom = uint8(firstZ)
	lastEntry := mergedEntries[len(mergedEntries)-1].Entry
	lastZ, _, _ := IDToZxy(lastEntry.TileID + uint64(lastEntry.RunLength) - 1)
	header.MaxZoom = uint8(lastZ)
	// construct a new center

	header.TileDataLength = acc
	header.AddressedTilesCount = addressedTiles
	header.TileEntriesCount = uint64(len(mergedEntries))
	header.TileContentsCount = tileContents.GetCardinality()

	// optimize IO by batching
	var mergeOps []MergeOp
	for _, me := range mergedEntries {
		last := len(mergeOps) - 1
		entryLength := uint64(me.Entry.Length)
		if last >= 0 && (mergeOps[last].InputIdx == me.InputIdx) && (me.InputOffset == mergeOps[last].InputOffset+mergeOps[last].Length) {
			mergeOps[last].Length += entryLength
		} else {
			mergeOps = append(mergeOps, MergeOp{InputIdx: me.InputIdx, InputOffset: me.InputOffset, Length: entryLength})
		}
	}

	output, _ := os.Create(inputs[len(inputs)-1])
	defer output.Close()

	headerBytes := SerializeHeader(header)
	_, _ = output.Write(headerBytes)
	_, _ = output.Write(rootBytes)
	fmt.Println("Copying JSON metadata from first input element")
	firstHandle := handles[0]
	firstHandle.Seek(int64(headers[0].MetadataOffset), io.SeekStart)
	io.CopyN(output, firstHandle, int64(headers[0].MetadataLength))
	_, _ = output.Write(leavesBytes)

	for _, op := range mergeOps {
		handle := handles[op.InputIdx]
		handle.Seek(int64(headers[op.InputIdx].TileDataOffset)+int64(op.InputOffset), io.SeekStart)
		io.CopyN(output, handle, int64(op.Length))
	}

	for _, h := range handles {
		h.Close()
	}

	return nil
}
