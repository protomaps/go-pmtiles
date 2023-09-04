package pmtiles

import (
	"bytes"
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/schollz/progressbar/v3"
	"gocloud.dev/blob"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strings"
)

// given a bitmap and a set of existing entries,
// create only relevant entries
// return sorted slice of entries, and slice of all leaf entries
// any runlengths > 1 will be "trimmed" to the relevance bitmap
func RelevantEntries(bitmap *roaring64.Bitmap, maxzoom uint8, dir []EntryV3) ([]EntryV3, []EntryV3) {
	last_tile := ZxyToId(maxzoom+1, 0, 0)
	leaves := make([]EntryV3, 0)
	tiles := make([]EntryV3, 0)
	for idx, entry := range dir {
		if entry.RunLength == 0 {
			tmp := roaring64.New()

			// if this is the last thing in the directory, it needs to be bounded
			if idx == len(dir)-1 {
				tmp.AddRange(entry.TileId, last_tile)
			} else {
				tmp.AddRange(entry.TileId, dir[idx+1].TileId)
			}

			if bitmap.Intersects(tmp) {
				leaves = append(leaves, entry)
			}
		} else if entry.RunLength == 1 {
			if bitmap.Contains(entry.TileId) {
				tiles = append(tiles, entry)
			}
		} else {
			// runlength > 1
			current_id := entry.TileId
			current_runlength := uint32(0)
			for y := entry.TileId; y < entry.TileId+uint64(entry.RunLength); y++ {
				if bitmap.Contains(y) {
					if current_runlength == 0 {
						current_runlength = 1
						current_id = y
					} else {
						current_runlength += 1
					}
				} else {
					if current_runlength > 0 {
						tiles = append(tiles, EntryV3{current_id, entry.Offset, entry.Length, current_runlength})
					}
					current_runlength = 0
				}
			}
			if current_runlength > 0 {
				tiles = append(tiles, EntryV3{current_id, entry.Offset, entry.Length, current_runlength})
			}
		}
	}
	return tiles, leaves
}

func ReencodeEntries(dir []EntryV3) ([]EntryV3, []Range) {
	reencoded := make([]EntryV3, 0, len(dir))
	seen_offsets := make(map[uint64]uint64)
	ranges := make([]Range, 0)
	offset := uint64(0)
	for _, entry := range dir {
		if val, ok := seen_offsets[entry.Offset]; ok {
			reencoded = append(reencoded, EntryV3{entry.TileId, val, entry.Length, entry.RunLength})
		} else {
			if len(ranges) > 0 {
				last_range := ranges[len(ranges)-1]
				if last_range.Offset+last_range.Length == entry.Offset {
					ranges[len(ranges)-1].Length += uint64(entry.Length)
				} else {
					ranges = append(ranges, Range{entry.Offset, uint64(entry.Length)})
				}
			} else {
				ranges = append(ranges, Range{entry.Offset, uint64(entry.Length)})
			}

			reencoded = append(reencoded, EntryV3{entry.TileId, offset, entry.Length, entry.RunLength})
			seen_offsets[entry.Offset] = offset
			offset += uint64(entry.Length)
		}
	}
	return reencoded, ranges
}

// "want the next N bytes, then discard N bytes"
type CopyDiscard struct {
	Wanted  uint64
	Discard uint64
}

type OverfetchRange struct {
	Rng          Range
	CopyDiscards []CopyDiscard
}

// A single request, where only some of the bytes
// in the requested range we want
type OverfetchListItem struct {
	Rng          Range
	CopyDiscards []CopyDiscard
	BytesToNext  uint64 // the "priority"
	prev         *OverfetchListItem
	next         *OverfetchListItem
	index        int
}

// overfetch = 0.2 means we can request an extra 20%
// overfetch = 1.00 means we can double our request size
// to minimize the # of HTTP requests
// the input ranges must be non-contiguous but might not be sorted
func MergeRanges(ranges []Range, overfetch float32) []OverfetchRange {
	total_size := 0

	list := make([]*OverfetchListItem, len(ranges))

	// create the heap items
	for i, rng := range ranges {
		var bytes_to_next uint64
		if i == len(ranges)-1 {
			bytes_to_next = math.MaxUint64
		} else {
			bytes_to_next = ranges[i+1].Offset - (rng.Offset + rng.Length)
			if bytes_to_next < 0 {
				bytes_to_next = math.MaxUint64
			}
		}

		list[i] = &OverfetchListItem{
			Rng:          rng,
			BytesToNext:  bytes_to_next,
			CopyDiscards: []CopyDiscard{{uint64(rng.Length), 0}},
			index:        i,
		}
		total_size += int(rng.Length)
	}

	// make the list doubly-linked
	for i, item := range list {
		if i > 0 {
			item.prev = list[i-1]
		}
		if i < len(list)-1 {
			item.next = list[i+1]
		}
	}

	overfetch_budget := int(float32(total_size) * overfetch)

	// create a 2nd slice, sorted by ascending distance to next range
	shortest := make([]*OverfetchListItem, len(list))
	copy(shortest, list)

	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].BytesToNext < shortest[j].BytesToNext
	})

	// while we haven't consumed the budget, merge ranges
	for (len(shortest) > 1) && (overfetch_budget-int(shortest[0].BytesToNext) >= 0) {
		item := shortest[0]

		// merge this item into item.next
		new_length := item.Rng.Length + item.BytesToNext + item.next.Rng.Length
		item.next.Rng = Range{item.Rng.Offset, new_length}
		item.next.prev = item.prev
		if item.prev != nil {
			item.prev.next = item.next
		}
		item.CopyDiscards[len(item.CopyDiscards)-1].Discard = item.BytesToNext
		item.next.CopyDiscards = append(item.CopyDiscards, item.next.CopyDiscards...)

		shortest = shortest[1:]

		overfetch_budget -= int(item.BytesToNext)
	}

	// copy out the result structs
	result := make([]OverfetchRange, len(shortest))

	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].index < shortest[j].index
	})

	for i, x := range shortest {
		result[i] = OverfetchRange{
			Rng:          x.Rng,
			CopyDiscards: x.CopyDiscards,
		}
	}

	return result
}

// 1. Get the root directory (check that it is clustered)
// 2. Turn the input geometry into a relevance bitmap (using min(maxzoom, headermaxzoom))
// 3. Get all relevant level 1 directories (if any)
// 4. Get all relevant level 2 directories (usually none)
// 5. With the existing directory + relevance bitmap, construct
//    * a new total directory (root + leaf directories)
//    * a sorted slice of byte ranges in the old file required
// 6. Merge requested ranges using an overfetch parametter
// 7. write the modified header
// 8. write the root directory.
// 9. get and write the metadata.
// 10. write the leaf directories (if any)
// 11. Get all tiles, and write directly to the output.
// (make this download multithreaded)

func Extract(logger *log.Logger, bucketURL string, file string, maxzoom uint8, region_file string, output string, overfetch float32) error {
	// 1. fetch the header

	if bucketURL == "" {
		if strings.HasPrefix(file, "/") {
			bucketURL = "file:///"
		} else {
			bucketURL = "file://"
		}
	}

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, file, 0, HEADERV3_LEN_BYTES, nil)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", file, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	r.Close()

	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])

	if !header.Clustered {
		return fmt.Errorf("Error: archive must be clustered for extracts.")
	}

	if header.MaxZoom < maxzoom || maxzoom == 0 {
		maxzoom = header.MaxZoom
	}

	// 2. construct a relevance bitmap
	dat, _ := ioutil.ReadFile(region_file)
	f, _ := geojson.UnmarshalFeature(dat)

	var multipolygon orb.MultiPolygon
	switch v := f.Geometry.(type) {
	case orb.Polygon:
		multipolygon = []orb.Polygon{v}
	case orb.MultiPolygon:
		multipolygon = v
	}

	bound := multipolygon.Bound()

	boundary_set, interior_set := bitmapMultiPolygon(maxzoom, multipolygon)

	relevant_set := boundary_set
	relevant_set.Or(interior_set)
	generalizeOr(relevant_set)

	// 3. get relevant entries from root
	dir_offset := header.RootOffset
	dir_length := header.RootLength

	root_reader, err := bucket.NewRangeReader(ctx, file, int64(dir_offset), int64(dir_length), nil)
	if err != nil {
		return err
	}
	defer root_reader.Close()
	root_bytes, err := io.ReadAll(root_reader)
	if err != nil {
		return err
	}

	root_dir := deserialize_entries(bytes.NewBuffer(root_bytes))

	tile_entries, leaves := RelevantEntries(relevant_set, maxzoom, root_dir)

	// 4. get all relevant leaf entries

	leaf_ranges := make([]Range, 0)
	for _, leaf := range leaves {
		leaf_ranges = append(leaf_ranges, Range{header.LeafDirectoryOffset + leaf.Offset, uint64(leaf.Length)})
	}

	overfetch_leaves := MergeRanges(leaf_ranges, overfetch)
	fmt.Printf("fetching %d dirs, %d chunks, %d requests\n", len(leaves), len(leaf_ranges), len(overfetch_leaves))

	for _, or := range overfetch_leaves {

		slab_r, err := bucket.NewRangeReader(ctx, file, int64(or.Rng.Offset), int64(or.Rng.Length), nil)
		if err != nil {
			return err
		}

		for _, cd := range or.CopyDiscards {

			leaf_bytes := make([]byte, cd.Wanted)
			_, err := io.ReadFull(slab_r, leaf_bytes)
			if err != nil {
				return err
			}
			leafdir := deserialize_entries(bytes.NewBuffer(leaf_bytes))
			new_entries, new_leaves := RelevantEntries(relevant_set, maxzoom, leafdir)

			if len(new_leaves) > 0 {
				panic("This doesn't support leaf level 2+.")
			}
			tile_entries = append(tile_entries, new_entries...)

			_, err = io.CopyN(io.Discard, slab_r, int64(cd.Discard))
			if err != nil {
				return err
			}
		}
		slab_r.Close()
	}

	sort.Slice(tile_entries, func(i, j int) bool {
		return tile_entries[i].TileId < tile_entries[j].TileId
	})

	fmt.Printf("Region tiles %d, result tile entries %d\n", relevant_set.GetCardinality(), len(tile_entries))

	// 6. create the new header and chunk list
	// we now need to re-encode this entry list using cumulative offsets
	reencoded, tile_parts := ReencodeEntries(tile_entries)

	fmt.Println("Reencoding done, parts: ", len(tile_parts))

	overfetch_ranges := MergeRanges(tile_parts, overfetch)
	fmt.Printf("fetching %d tiles, %d chunks, %d requests\n", len(reencoded), len(tile_parts), len(overfetch_ranges))

	// construct the directories
	new_root_bytes, new_leaves_bytes, _ := optimize_directories(reencoded, 16384-HEADERV3_LEN_BYTES)

	// 7. write the modified header
	header.RootOffset = HEADERV3_LEN_BYTES
	header.RootLength = uint64(len(new_root_bytes))
	old_metadata_offset := header.MetadataOffset
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(new_leaves_bytes))
	old_tile_data_offset := header.TileDataOffset
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength
	last_part := tile_parts[len(tile_parts)-1]
	header.TileDataLength = last_part.Offset + uint64(last_part.Length)

	//TODO set statistics

	header.MinLonE7 = int32(bound.Left() * 10000000)
	header.MinLatE7 = int32(bound.Bottom() * 10000000)
	header.MaxLonE7 = int32(bound.Right() * 10000000)
	header.MaxLatE7 = int32(bound.Top() * 10000000)
	header.CenterLonE7 = int32(bound.Center().X() * 10000000)
	header.CenterLatE7 = int32(bound.Center().Y() * 10000000)
	header.MaxZoom = maxzoom

	header_bytes := serialize_header(header)

	outfile, err := os.Create(output)
	defer outfile.Close()
	_, err = outfile.Write(header_bytes)
	if err != nil {
		return err
	}

	// 8. write the root directory
	_, err = outfile.Write(new_root_bytes)
	if err != nil {
		return err
	}

	// 9. get and write the metadata
	metadata_reader, err := bucket.NewRangeReader(ctx, file, int64(old_metadata_offset), int64(header.MetadataLength), nil)
	if err != nil {
		return err
	}
	metadata_bytes, err := io.ReadAll(metadata_reader)
	defer metadata_reader.Close()
	if err != nil {
		return err
	}

	outfile.Write(metadata_bytes)

	// 10. write the leaf directories
	_, err = outfile.Write(new_leaves_bytes)
	if err != nil {
		return err
	}

	total_bytes := uint64(0)
	for _, x := range tile_parts {
		total_bytes += x.Length
	}

	bar := progressbar.DefaultBytes(
		int64(total_bytes),
		"downloading " + output,
	)
	for _, or := range overfetch_ranges {

		tile_r, err := bucket.NewRangeReader(ctx, file, int64(old_tile_data_offset+or.Rng.Offset), int64(or.Rng.Length), nil)
		if err != nil {
			return err
		}

		for _, cd := range or.CopyDiscards {
			_, err := io.CopyN(io.MultiWriter(outfile, bar), tile_r, int64(cd.Wanted))
			if err != nil {
				return err
			}

			_, err = io.CopyN(io.MultiWriter(io.Discard,bar), tile_r, int64(cd.Discard))
			if err != nil {
				return err
			}
		}
		tile_r.Close()
	}

	return nil
}
