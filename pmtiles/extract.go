package pmtiles

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dustin/go-humanize"
	"github.com/paulmach/orb"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

type SrcDstRange struct {
	SrcOffset uint64
	DstOffset uint64
	Length    uint64
}

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

// Given a tile entries for a Source archive, sorted in TileID order,
// return:
// * Re-encoded tile-entries, with their offsets changed to contiguous (clustered) order in a new archive.
// * SrcDstRange: slice of offsets in the source archive, offset in the new archive, and length.
//   - Each range is one or more tiles
//   - the output must not have contiguous entries
//   - It is sorted by new offsets, but not necessarily by source offsets
//
// * The total size of the tile section in the new archive
// * The # of addressed tiles (sum over RunLength)
// * # the number of unique offsets ("tile contents")
//   - this might not be the last SrcDstRange new_offset + length, it's the highest offset (can be in the middle)
func ReencodeEntries(dir []EntryV3) ([]EntryV3, []SrcDstRange, uint64, uint64, uint64) {
	reencoded := make([]EntryV3, 0, len(dir))
	seen_offsets := make(map[uint64]uint64)
	ranges := make([]SrcDstRange, 0)
	addressed_tiles := uint64(0)

	dst_offset := uint64(0)
	for _, entry := range dir {
		if val, ok := seen_offsets[entry.Offset]; ok {
			reencoded = append(reencoded, EntryV3{entry.TileId, val, entry.Length, entry.RunLength})
		} else {
			if len(ranges) > 0 {
				last_range := ranges[len(ranges)-1]
				if last_range.SrcOffset+last_range.Length == entry.Offset {
					ranges[len(ranges)-1].Length += uint64(entry.Length)
				} else {
					ranges = append(ranges, SrcDstRange{entry.Offset, dst_offset, uint64(entry.Length)})
				}
			} else {
				ranges = append(ranges, SrcDstRange{entry.Offset, dst_offset, uint64(entry.Length)})
			}

			reencoded = append(reencoded, EntryV3{entry.TileId, dst_offset, entry.Length, entry.RunLength})
			seen_offsets[entry.Offset] = dst_offset
			dst_offset += uint64(entry.Length)
		}

		addressed_tiles += uint64(entry.RunLength)
	}
	return reencoded, ranges, dst_offset, addressed_tiles, uint64(len(seen_offsets))
}

// "want the next N bytes, then discard N bytes"
type CopyDiscard struct {
	Wanted  uint64
	Discard uint64
}

type OverfetchRange struct {
	Rng          SrcDstRange
	CopyDiscards []CopyDiscard
}

// A single request, where only some of the bytes
// in the requested range we want
type OverfetchListItem struct {
	Rng          SrcDstRange
	CopyDiscards []CopyDiscard
	BytesToNext  uint64 // the "priority"
	prev         *OverfetchListItem
	next         *OverfetchListItem
	index        int
}

// MergeRanges takes a slice of SrcDstRanges, that:
// * is non-contiguous, and is sorted by NewOffset
// * an Overfetch parameter
//   - overfetch = 0.2 means we can request an extra 20%
//   - overfetch = 1.00 means we can double our total transfer size
//
// Return a slice of OverfetchRanges
//
//	Each OverfetchRange is one or more input ranges
//	input ranges are merged in order of smallest byte distance to next range
//	until the overfetch budget is consumed.
//	The slice is sorted by Length
func MergeRanges(ranges []SrcDstRange, overfetch float32) (*list.List, uint64) {
	total_size := 0

	shortest := make([]*OverfetchListItem, len(ranges))

	// create the heap items
	for i, rng := range ranges {
		var bytes_to_next uint64
		if i == len(ranges)-1 {
			bytes_to_next = math.MaxUint64
		} else {
			bytes_to_next = ranges[i+1].SrcOffset - (rng.SrcOffset + rng.Length)
			if bytes_to_next < 0 {
				bytes_to_next = math.MaxUint64
			}
		}

		shortest[i] = &OverfetchListItem{
			Rng:          rng,
			BytesToNext:  bytes_to_next,
			CopyDiscards: []CopyDiscard{{uint64(rng.Length), 0}},
		}
		total_size += int(rng.Length)
	}

	// make the list doubly-linked
	for i, item := range shortest {
		if i > 0 {
			item.prev = shortest[i-1]
		}
		if i < len(shortest)-1 {
			item.next = shortest[i+1]
		}
	}

	overfetch_budget := int(float32(total_size) * overfetch)

	// sort by ascending distance to next range
	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].BytesToNext < shortest[j].BytesToNext
	})

	// while we haven't consumed the budget, merge ranges
	for (len(shortest) > 1) && (overfetch_budget-int(shortest[0].BytesToNext) >= 0) {
		item := shortest[0]

		// merge this item into item.next
		new_length := item.Rng.Length + item.BytesToNext + item.next.Rng.Length
		item.next.Rng = SrcDstRange{item.Rng.SrcOffset, item.Rng.DstOffset, new_length}
		item.next.prev = item.prev
		if item.prev != nil {
			item.prev.next = item.next
		}
		item.CopyDiscards[len(item.CopyDiscards)-1].Discard = item.BytesToNext
		item.next.CopyDiscards = append(item.CopyDiscards, item.next.CopyDiscards...)

		shortest = shortest[1:]

		overfetch_budget -= int(item.BytesToNext)
	}

	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].Rng.Length > shortest[j].Rng.Length
	})

	total_bytes := uint64(0)
	result := list.New()
	for _, x := range shortest {
		result.PushBack(OverfetchRange{
			Rng:          x.Rng,
			CopyDiscards: x.CopyDiscards,
		})
		total_bytes += x.Rng.Length
	}

	return result, total_bytes
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

func Extract(logger *log.Logger, bucketURL string, key string, maxzoom int8, region_file string, bbox string, output string, download_threads int, overfetch float32, dry_run bool) error {
	// 1. fetch the header
	start := time.Now()
	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey(bucketURL, "", key)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return err
	}

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, HEADERV3_LEN_BYTES)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	r.Close()

	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])

	if !header.Clustered {
		return fmt.Errorf("Error: source archive must be clustered for extracts.")
	}

	source_metadata_offset := header.MetadataOffset
	source_tile_data_offset := header.TileDataOffset

	if maxzoom == -1 || int8(header.MaxZoom) < maxzoom {
		maxzoom = int8(header.MaxZoom)
	}

	var relevant_set *roaring64.Bitmap
	if region_file != "" || bbox != "" {
		if region_file != "" && bbox != "" {
			return fmt.Errorf("Only one of region and bbox can be specified.")
		}

		var multipolygon orb.MultiPolygon

		if region_file != "" {
			dat, _ := ioutil.ReadFile(region_file)
			multipolygon, err = UnmarshalRegion(dat)

			if err != nil {
				return err
			}
		} else {
			multipolygon, err = BboxRegion(bbox)
			if err != nil {
				return err
			}
		}

		// 2. construct a relevance bitmap

		bound := multipolygon.Bound()

		boundary_set, interior_set := bitmapMultiPolygon(uint8(maxzoom), multipolygon)
		relevant_set = boundary_set
		relevant_set.Or(interior_set)
		generalizeOr(relevant_set)

		header.MinLonE7 = int32(bound.Left() * 10000000)
		header.MinLatE7 = int32(bound.Bottom() * 10000000)
		header.MaxLonE7 = int32(bound.Right() * 10000000)
		header.MaxLatE7 = int32(bound.Top() * 10000000)
		header.CenterLonE7 = int32(bound.Center().X() * 10000000)
		header.CenterLatE7 = int32(bound.Center().Y() * 10000000)
	} else {
		relevant_set = roaring64.New()
		relevant_set.AddRange(0, ZxyToId(uint8(maxzoom)+1, 0, 0))
	}

	// 3. get relevant entries from root
	dir_offset := header.RootOffset
	dir_length := header.RootLength

	root_reader, err := bucket.NewRangeReader(ctx, key, int64(dir_offset), int64(dir_length))
	if err != nil {
		return err
	}
	defer root_reader.Close()
	root_bytes, err := io.ReadAll(root_reader)
	if err != nil {
		return err
	}

	root_dir := deserialize_entries(bytes.NewBuffer(root_bytes))

	tile_entries, leaves := RelevantEntries(relevant_set, uint8(maxzoom), root_dir)

	// 4. get all relevant leaf entries

	leaf_ranges := make([]SrcDstRange, 0)
	for _, leaf := range leaves {
		leaf_ranges = append(leaf_ranges, SrcDstRange{header.LeafDirectoryOffset + leaf.Offset, 0, uint64(leaf.Length)})
	}

	overfetch_leaves, _ := MergeRanges(leaf_ranges, overfetch)
	num_overfetch_leaves := overfetch_leaves.Len()
	fmt.Printf("fetching %d dirs, %d chunks, %d requests\n", len(leaves), len(leaf_ranges), overfetch_leaves.Len())

	for {
		if overfetch_leaves.Len() == 0 {
			break
		}
		or := overfetch_leaves.Remove(overfetch_leaves.Front()).(OverfetchRange)

		slab_r, err := bucket.NewRangeReader(ctx, key, int64(or.Rng.SrcOffset), int64(or.Rng.Length))
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
			new_entries, new_leaves := RelevantEntries(relevant_set, uint8(maxzoom), leafdir)

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
	reencoded, tile_parts, tiledata_length, addressed_tiles, tile_contents := ReencodeEntries(tile_entries)

	overfetch_ranges, total_bytes := MergeRanges(tile_parts, overfetch)

	num_overfetch_ranges := overfetch_ranges.Len()
	fmt.Printf("fetching %d tiles, %d chunks, %d requests\n", len(reencoded), len(tile_parts), overfetch_ranges.Len())

	// TODO: takes up too much RAM
	// construct the directories
	new_root_bytes, new_leaves_bytes, _ := optimize_directories(reencoded, 16384-HEADERV3_LEN_BYTES)

	// 7. write the modified header
	header.RootOffset = HEADERV3_LEN_BYTES
	header.RootLength = uint64(len(new_root_bytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(new_leaves_bytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength

	header.TileDataLength = tiledata_length
	header.AddressedTilesCount = addressed_tiles
	header.TileEntriesCount = uint64(len(tile_entries))
	header.TileContentsCount = tile_contents

	header.MaxZoom = uint8(maxzoom)

	header_bytes := serialize_header(header)

	total_actual_bytes := uint64(0)
	for _, x := range tile_parts {
		total_actual_bytes += x.Length
	}

	if !dry_run {

		outfile, err := os.Create(output)
		defer outfile.Close()

		outfile.Truncate(127 + int64(len(new_root_bytes)) + int64(header.MetadataLength) + int64(len(new_leaves_bytes)) + int64(total_actual_bytes))

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
		metadata_reader, err := bucket.NewRangeReader(ctx, key, int64(source_metadata_offset), int64(header.MetadataLength))
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

		bar := progressbar.DefaultBytes(
			int64(total_bytes),
			"fetching chunks",
		)

		var mu sync.Mutex

		downloadPart := func(or OverfetchRange) error {
			tile_r, err := bucket.NewRangeReader(ctx, key, int64(source_tile_data_offset+or.Rng.SrcOffset), int64(or.Rng.Length))
			if err != nil {
				return err
			}
			offset_writer := io.NewOffsetWriter(outfile, int64(header.TileDataOffset)+int64(or.Rng.DstOffset))

			for _, cd := range or.CopyDiscards {

				_, err := io.CopyN(io.MultiWriter(offset_writer, bar), tile_r, int64(cd.Wanted))
				if err != nil {
					return err
				}

				_, err = io.CopyN(bar, tile_r, int64(cd.Discard))
				if err != nil {
					return err
				}
			}
			tile_r.Close()
			return nil
		}

		errs, _ := errgroup.WithContext(ctx)

		for i := 0; i < download_threads; i++ {
			work_back := (i == 0 && download_threads > 1)
			errs.Go(func() error {
				done := false
				var or OverfetchRange
				for {
					mu.Lock()
					if overfetch_ranges.Len() == 0 {
						done = true
					} else {
						if work_back {
							or = overfetch_ranges.Remove(overfetch_ranges.Back()).(OverfetchRange)
						} else {
							or = overfetch_ranges.Remove(overfetch_ranges.Front()).(OverfetchRange)
						}
					}
					mu.Unlock()
					if done {
						return nil
					}
					err := downloadPart(or)
					if err != nil {
						return err
					}
				}

				return nil
			})
		}

		err = errs.Wait()
		if err != nil {
			return err
		}
	}

	fmt.Printf("Completed in %v with %v download threads (%v tiles/s).\n", time.Since(start), download_threads, float64(len(reencoded))/float64(time.Since(start).Seconds()))
	total_requests := 2                    // header + root
	total_requests += num_overfetch_leaves // leaves
	total_requests += 1                    // metadata
	total_requests += num_overfetch_ranges
	fmt.Printf("Extract required %d total requests.\n", total_requests)
	fmt.Printf("Extract transferred %s (overfetch %v) for an archive size of %s\n", humanize.Bytes(total_bytes), overfetch, humanize.Bytes(total_actual_bytes))

	return nil
}
