package pmtiles

import (
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"sort"
)

type MergeEntry struct {
	Entry       EntryV3
	InputIdx    int    // the index of the input archive 0...N
	InputOffset uint64 // the original offset of the entry in the archive's tile section
}

type MergeOp struct {
	InputIdx int
	Length   uint64
}

type Remapping struct {
	SrcOffset uint64
	DstOffset uint64
}

// load N archives, validating that they are mergeable and disjoint.
// returns a formatted error and index of the mismatched archive if not.
// if valid, retruns a sorted list of MergeEntry records, each containing a directory Entry
// but with offset values referring to the original input archive.
func prepareInputs(inputs []io.ReadSeeker) ([]HeaderV3, []MergeEntry, error, int) {
	var headers []HeaderV3
	var mergedEntries []MergeEntry
	union := roaring64.New()

	for inputIdx, input := range inputs {
		buf := make([]byte, HeaderV3LenBytes)
		_, err := input.Read(buf)
		if err != nil {
			return nil, nil, err, inputIdx
		}
		h, err := DeserializeHeader(buf)
		if err != nil {
			return nil, nil, err, inputIdx
		}
		headers = append(headers, h)

		// also validate the headers so we "fail fast"
		if !h.Clustered {
			return nil, nil, fmt.Errorf("is not clustered"), inputIdx
		}

		if inputIdx > 0 {
			if h.TileType != headers[0].TileType {
				return nil, nil, fmt.Errorf("tile type %s does not match %s", tileTypeToString(h.TileType), tileTypeToString(headers[0].TileType)), inputIdx
			}
			if h.TileCompression != headers[0].TileCompression {
				c1, _ := compressionToString(h.TileCompression)
				c2, _ := compressionToString(headers[0].TileCompression)
				return nil, nil, fmt.Errorf("tile compression %s does not match %s", c1, c2), inputIdx
			}
			if h.InternalCompression != headers[0].InternalCompression {
				c1, _ := compressionToString(h.InternalCompression)
				c2, _ := compressionToString(headers[0].InternalCompression)
				return nil, nil, fmt.Errorf("internal compression %s does not match %s", c1, c2), inputIdx
			}
		}

		tileset := roaring64.New()
		err = IterateEntries(h,
			func(offset uint64, length uint64) ([]byte, error) {
				input.Seek(int64(offset), io.SeekStart)
				return io.ReadAll(io.LimitReader(input, int64(length)))
			},
			func(e EntryV3) {
				tileset.AddRange(e.TileID, e.TileID+uint64(e.RunLength))
				mergedEntries = append(mergedEntries, MergeEntry{Entry: e, InputOffset: e.Offset, InputIdx: inputIdx})
			})

		if err != nil {
			return nil, nil, err, inputIdx
		}

		if union.Intersects(tileset) {
			tmp := union.Clone()
			tmp.And(tileset)
			iz, ix, iy := IDToZxy(tmp.Minimum())
			return nil, nil, fmt.Errorf("%d overlapping tiles, starting with %d %d %d", tmp.GetCardinality(), iz, ix, iy), inputIdx
		}
		union.Or(tileset)
	}

	sort.Slice(mergedEntries, func(i, j int) bool {
		return mergedEntries[i].Entry.TileID < mergedEntries[j].Entry.TileID
	})

	return headers, mergedEntries, nil, 0
}

// remaps a sorted slice of MergeEntry
// changes each Entry to be contiguous in the new archive.
// also handles deduplicated backreferences
func remapMergeEntries(entries []MergeEntry, numInputs int) ([]MergeEntry, uint64, uint64, uint64, error) {
	acc := uint64(0)
	addressedTiles := uint64(0)
	tileContents := 0
	remappings := make([][]Remapping, numInputs)

	for idx, me := range entries {
		remapping := remappings[me.InputIdx]
		if len(remapping) > 0 && me.InputOffset < remappings[me.InputIdx][len(remapping)-1].SrcOffset {
			// find the original offset in the remapping slice
			i, ok := slices.BinarySearchFunc(remapping, me.InputOffset, func(r Remapping, k uint64) int {
				switch {
				case r.SrcOffset < k:
					return -1
				case r.SrcOffset > k:
					return 1
				default:
					return 0
				}
			})
			if ok {
				entries[idx].Entry.Offset = remapping[i].DstOffset
			} else {
				return nil, 0, 0, 0, fmt.Errorf("clustered archive has out-of-order entries")
			}
		} else {
			entries[idx].Entry.Offset = acc
			remappings[me.InputIdx] = append(remappings[me.InputIdx], Remapping{SrcOffset: me.InputOffset, DstOffset: acc})
			acc += uint64(me.Entry.Length)
			tileContents += 1
		}

		addressedTiles += uint64(entries[idx].Entry.RunLength)
	}
	return entries, addressedTiles, uint64(tileContents), acc, nil
}

// combines contiguous I/O operations and eliminate backreferences from the copy operation.
func batchMergeEntries(entries []MergeEntry, numInputs int) []MergeOp {
	lastOffset := make([]uint64, numInputs)
	var mergeOps []MergeOp
	for _, me := range entries {
		if me.InputOffset < lastOffset[me.InputIdx] {
			continue
		}
		last := len(mergeOps) - 1
		entryLength := uint64(me.Entry.Length)
		if last >= 0 && (mergeOps[last].InputIdx == me.InputIdx) && (me.InputOffset == lastOffset[me.InputIdx]+mergeOps[last].Length) {
			mergeOps[last].Length += entryLength
		} else {
			mergeOps = append(mergeOps, MergeOp{InputIdx: me.InputIdx, Length: entryLength})
		}
		lastOffset[me.InputIdx] = me.InputOffset
	}
	return mergeOps
}

func zoomBounds(entries []MergeEntry) (uint8, uint8) {
	firstZ, _, _ := IDToZxy(entries[0].Entry.TileID)
	lastEntry := entries[len(entries)-1].Entry
	lastZ, _, _ := IDToZxy(lastEntry.TileID + uint64(lastEntry.RunLength) - 1)
	return uint8(firstZ), uint8(lastZ)
}

func bounds(headers []HeaderV3) (int32, int32, int32, int32) {
	minLonE7 := int32(math.MaxInt32)
	minLatE7 := int32(math.MaxInt32)
	maxLonE7 := int32(math.MinInt32)
	maxLatE7 := int32(math.MinInt32)

	for _, h := range headers {
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
	}

	return minLonE7, minLatE7, maxLonE7, maxLatE7
}

func Merge(logger *log.Logger, inputs []string) error {
	var handles []io.ReadSeeker

	for _, name := range inputs[:len(inputs)-1] {
		f, err := os.OpenFile(name, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		handles = append(handles, f)
		defer f.Close()
	}

	headers, mergedEntries, err, errIdx := prepareInputs(handles)
	if err != nil {
		return fmt.Errorf("%s: %w", inputs[errIdx], err)
	}

	renumberedEntries, addressedTiles, numTileContents, tileDataLength, err := remapMergeEntries(mergedEntries, len(headers))
	if err != nil {
		return err
	}

	tmp := make([]EntryV3, len(renumberedEntries))
	for i := range renumberedEntries {
		tmp[i] = renumberedEntries[i].Entry
	}
	rootBytes, leavesBytes, _ := optimizeDirectories(tmp, 16384-HeaderV3LenBytes, Gzip)

	logger.Printf("Copying center and JSON metadata from first input %s", inputs[0])

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
	header.TileDataLength = tileDataLength
	header.AddressedTilesCount = addressedTiles
	header.TileEntriesCount = uint64(len(renumberedEntries))
	header.TileContentsCount = numTileContents

	minZoom, maxZoom := zoomBounds(renumberedEntries)
	header.MinZoom = minZoom
	header.MaxZoom = maxZoom
	minLonE7, minLatE7, maxLonE7, maxLatE7 := bounds(headers)
	header.MinLonE7 = minLonE7
	header.MinLatE7 = minLatE7
	header.MaxLonE7 = maxLonE7
	header.MaxLatE7 = maxLatE7
	header.CenterLonE7 = headers[0].CenterLonE7
	header.CenterLatE7 = headers[0].CenterLatE7
	header.CenterZoom = headers[0].CenterZoom

	mergeOps := batchMergeEntries(renumberedEntries, len(headers))

	output, err := os.Create(inputs[len(inputs)-1])
	if err != nil {
		return err
	}
	defer output.Close()

	headerBytes := SerializeHeader(header)
	_, err = output.Write(headerBytes)
	if err != nil {
		return err
	}
	_, err = output.Write(rootBytes)
	if err != nil {
		return err
	}
	firstHandle := handles[0]
	firstHandle.Seek(int64(headers[0].MetadataOffset), io.SeekStart)
	io.CopyN(output, firstHandle, int64(headers[0].MetadataLength))

	_, err = output.Write(leavesBytes)
	if err != nil {
		return err
	}

	for _, handle := range handles {
		handle.Seek(0, io.SeekStart)
	}

	for _, op := range mergeOps {
		handle := handles[op.InputIdx]
		_, err := io.CopyN(output, handle, int64(op.Length))
		if err != nil {
			return err
		}
	}

	return nil
}
