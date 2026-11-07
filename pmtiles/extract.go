package pmtiles

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dustin/go-humanize"
	"github.com/paulmach/orb"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type srcDstRange struct {
	SrcOffset uint64
	DstOffset uint64
	Length    uint64
}

// RelevantEntries finds the intersection of a bitmap and a directory
// return sorted slice of entries, and slice of all leaf entries
// any runlengths > 1 will be "trimmed" to the relevance bitmap
func RelevantEntries(bitmap *roaring64.Bitmap, maxzoom uint8, dir []EntryV3) ([]EntryV3, []EntryV3) {
	lastTile := ZxyToID(maxzoom+1, 0, 0)
	leaves := make([]EntryV3, 0)
	tiles := make([]EntryV3, 0)
	for idx, entry := range dir {
		if entry.RunLength == 0 {
			tmp := roaring64.New()

			// if this is the last thing in the directory, it needs to be bounded
			if idx == len(dir)-1 {
				tmp.AddRange(entry.TileID, lastTile)
			} else {
				tmp.AddRange(entry.TileID, dir[idx+1].TileID)
			}

			if bitmap.Intersects(tmp) {
				leaves = append(leaves, entry)
			}
		} else if entry.RunLength == 1 {
			if bitmap.Contains(entry.TileID) {
				tiles = append(tiles, entry)
			}
		} else {
			// runlength > 1
			currentID := entry.TileID
			currentRunLength := uint32(0)
			for y := entry.TileID; y < entry.TileID+uint64(entry.RunLength); y++ {
				if bitmap.Contains(y) {
					if currentRunLength == 0 {
						currentRunLength = 1
						currentID = y
					} else {
						currentRunLength++
					}
				} else {
					if currentRunLength > 0 {
						tiles = append(tiles, EntryV3{currentID, entry.Offset, entry.Length, currentRunLength})
					}
					currentRunLength = 0
				}
			}
			if currentRunLength > 0 {
				tiles = append(tiles, EntryV3{currentID, entry.Offset, entry.Length, currentRunLength})
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
func reencodeEntries(dir []EntryV3) ([]EntryV3, []srcDstRange, uint64, uint64, uint64) {
	reencoded := make([]EntryV3, 0, len(dir))
	seenOffsets := make(map[uint64]uint64)
	ranges := make([]srcDstRange, 0)
	addressedTiles := uint64(0)

	dstOffset := uint64(0)
	for _, entry := range dir {
		if val, ok := seenOffsets[entry.Offset]; ok {
			reencoded = append(reencoded, EntryV3{entry.TileID, val, entry.Length, entry.RunLength})
		} else {
			if len(ranges) > 0 {
				lastRange := ranges[len(ranges)-1]
				if lastRange.SrcOffset+lastRange.Length == entry.Offset {
					ranges[len(ranges)-1].Length += uint64(entry.Length)
				} else {
					ranges = append(ranges, srcDstRange{entry.Offset, dstOffset, uint64(entry.Length)})
				}
			} else {
				ranges = append(ranges, srcDstRange{entry.Offset, dstOffset, uint64(entry.Length)})
			}

			reencoded = append(reencoded, EntryV3{entry.TileID, dstOffset, entry.Length, entry.RunLength})
			seenOffsets[entry.Offset] = dstOffset
			dstOffset += uint64(entry.Length)
		}

		addressedTiles += uint64(entry.RunLength)
	}
	return reencoded, ranges, dstOffset, addressedTiles, uint64(len(seenOffsets))
}

// "want the next N bytes, then discard N bytes"
type copyDiscard struct {
	Wanted  uint64
	Discard uint64
}

type overfetchRange struct {
	Rng          srcDstRange
	CopyDiscards []copyDiscard
}

// A single request, where only some of the bytes
// in the requested range we want
type overfetchListItem struct {
	Rng          srcDstRange
	CopyDiscards []copyDiscard
	BytesToNext  uint64 // the "priority"
	prev         *overfetchListItem
	next         *overfetchListItem
	index        int
}

// mergeRanges takes a slice of SrcDstRanges, that:
// * is non-contiguous, and is sorted by DstOffset
// * an Overfetch parameter
//   - overfetch = 0.2 means we can request an extra 20%
//   - overfetch = 1.00 means we can double our total transfer size
//
// Return a list of OverfetchRanges
//
//	Each OverfetchRange is one or more input ranges
//	input ranges are merged in order of smallest byte distance to next range
//	until the overfetch budget is consumed.
//	The list is sorted by Length
func mergeRanges(ranges []srcDstRange, overfetch float32) (*list.List, uint64) {
	totalSize := 0

	shortest := make([]*overfetchListItem, len(ranges))

	// create the heap items
	for i, rng := range ranges {
		var bytesToNext int64
		if i == len(ranges)-1 {
			bytesToNext = math.MaxInt64
		} else {
			bytesToNext = int64(ranges[i+1].SrcOffset) - (int64(rng.SrcOffset) + int64(rng.Length))
			if bytesToNext < 0 {
				bytesToNext = math.MaxInt64
			}
		}

		shortest[i] = &overfetchListItem{
			Rng:          rng,
			BytesToNext:  uint64(bytesToNext),
			CopyDiscards: []copyDiscard{{uint64(rng.Length), 0}},
		}
		totalSize += int(rng.Length)
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

	overfetchBudget := int(float32(totalSize) * overfetch)

	// sort by ascending distance to next range
	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].BytesToNext < shortest[j].BytesToNext
	})

	// while we haven't consumed the budget, merge ranges
	for (len(shortest) > 1) && (overfetchBudget-int(shortest[0].BytesToNext) >= 0) {
		item := shortest[0]

		// merge this item into item.next
		newLength := item.Rng.Length + item.BytesToNext + item.next.Rng.Length
		item.next.Rng = srcDstRange{item.Rng.SrcOffset, item.Rng.DstOffset, newLength}
		item.next.prev = item.prev
		if item.prev != nil {
			item.prev.next = item.next
		}
		item.CopyDiscards[len(item.CopyDiscards)-1].Discard = item.BytesToNext
		item.next.CopyDiscards = append(item.CopyDiscards, item.next.CopyDiscards...)

		shortest = shortest[1:]

		overfetchBudget -= int(item.BytesToNext)
	}

	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].Rng.Length > shortest[j].Rng.Length
	})

	totalBytes := uint64(0)
	result := list.New()
	for _, x := range shortest {
		result.PushBack(overfetchRange{
			Rng:          x.Rng,
			CopyDiscards: x.CopyDiscards,
		})
		totalBytes += x.Rng.Length
	}

	return result, totalBytes
}

// Extract a smaller archive from local or remote archive.
// 1. Get the root directory (check that it is clustered)
// 2. Turn the input geometry into a relevance bitmap (using min(maxzoom, headermaxzoom))
// 3. Get all relevant level 1 directories (if any)
// 4. Get all relevant level 2 directories (usually none)
// 5. With the existing directory + relevance bitmap, construct
//   - a new total directory (root + leaf directories)
//   - a sorted slice of byte ranges in the old file required
//
// 6. Merge requested ranges using an overfetch parametter
// 7. write the modified header
// 8. write the root directory.
// 9. get and write the metadata.
// 10. write the leaf directories (if any)
// 11. Get all tiles, and write directly to the output.
//
// Extract is ExtractWithResume with resume turned off.
// It keeps its signature, so that code that calls it does not change.
func Extract(ctx context.Context, logger *log.Logger, bucketURL string, key string, minzoom int8, maxzoom int8, regionFile string, bbox string, output string, downloadThreads int, overfetch float32, dryRun bool) error {
	return ExtractWithResume(ctx, logger, bucketURL, key, minzoom, maxzoom, regionFile, bbox, output, downloadThreads, overfetch, dryRun, ResumeOptions{})
}

// ExtractWithResume is Extract, with an extract that can continue after an interruption.
// With resume.StatePath set, it records the progress of the download in a resume state file.
// A later call with the same source and arguments continues from the recorded progress.
// See ResumeOptions.
//
// Design decision: ExtractWithResume is a separate function, and Extract keeps its signature.
// Extract is public API with 12 positional parameters. More parameters on Extract would break
// every program that calls it. Extract calls ExtractWithResume with resume turned off.
func ExtractWithResume(ctx context.Context, logger *log.Logger, bucketURL string, key string, minzoom int8, maxzoom int8, regionFile string, bbox string, output string, downloadThreads int, overfetch float32, dryRun bool, resume ResumeOptions) error {
	// 1. fetch the header
	start := time.Now()

	if ctx == nil {
		ctx = context.Background()
	}

	bucketURL, key, err := NormalizeBucketKey(bucketURL, "", key)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	// Every later read of the source sends the ETag of this first read with If-Match,
	// so a source that changes during the extract fails the read instead of mixing two versions.
	r, sourceEtag, _, err := bucket.NewRangeReaderEtag(ctx, key, 0, HeaderV3LenBytes, "")

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	r.Close()

	readSource := func(offset int64, length int64) (io.ReadCloser, error) {
		reader, _, _, err := bucket.NewRangeReaderEtag(ctx, key, offset, length, sourceEtag)
		return reader, err
	}

	header, err := DeserializeHeader(b[0:HeaderV3LenBytes])

	if !header.Clustered {
		return fmt.Errorf("source archive must be clustered for extracts")
	}

	sourceMetadataOffset := header.MetadataOffset
	sourceTileDataOffset := header.TileDataOffset

	if minzoom == -1 || int8(header.MinZoom) > minzoom {
		minzoom = int8(header.MinZoom)
	}

	if maxzoom == -1 || int8(header.MaxZoom) < maxzoom {
		maxzoom = int8(header.MaxZoom)
	}

	if minzoom > maxzoom {
		return fmt.Errorf("minzoom cannot be greater than maxzoom")
	}

	var relevantSet *roaring64.Bitmap
	if regionFile != "" || bbox != "" {
		if regionFile != "" && bbox != "" {
			return fmt.Errorf("only one of region and bbox can be specified")
		}

		var multipolygon orb.MultiPolygon
		var centerLon, centerLat float64

		if regionFile != "" {
			dat, _ := ioutil.ReadFile(regionFile)
			multipolygon, err = UnmarshalRegion(dat)
			if err != nil {
				return err
			}

			bound := multipolygon.Bound()
			centerLon = bound.Center().X()
			centerLat = bound.Center().Y()
		} else {
			parts := strings.Split(bbox, ",")
			minLon, err := strconv.ParseFloat(parts[0], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}
			minLat, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}
			maxLon, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}
			maxLat, err := strconv.ParseFloat(parts[3], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}

			multipolygon = bboxToMultiPolygon(minLon, minLat, maxLon, maxLat)

			centerLon = (minLon + maxLon) / 2
			if minLon > maxLon {
				centerLon = (minLon + maxLon + 360) / 2
				if centerLon > 180 {
					centerLon -= 360
				}
			}
			centerLat = (minLat + maxLat) / 2
		}

		bound := multipolygon.Bound()
		header.MinLonE7 = int32(bound.Left() * 10000000)
		header.MinLatE7 = int32(bound.Bottom() * 10000000)
		header.MaxLonE7 = int32(bound.Right() * 10000000)
		header.MaxLatE7 = int32(bound.Top() * 10000000)
		header.CenterLonE7 = int32(centerLon * 10000000)
		header.CenterLatE7 = int32(centerLat * 10000000)

		// 2. construct a relevance bitmap
		boundarySet, interiorSet := bitmapMultiPolygon(uint8(maxzoom), multipolygon)
		relevantSet = boundarySet
		relevantSet.Or(interiorSet)
		generalizeOr(relevantSet, uint8(minzoom))
	} else {
		relevantSet = roaring64.New()
		relevantSet.AddRange(ZxyToID(uint8(minzoom), 0, 0), ZxyToID(uint8(maxzoom)+1, 0, 0))
	}

	// 3. get relevant entries from root
	dirOffset := header.RootOffset
	dirLength := header.RootLength

	rootReader, err := readSource(int64(dirOffset), int64(dirLength))
	if err != nil {
		return err
	}
	defer rootReader.Close()
	rootBytes, err := io.ReadAll(rootReader)
	if err != nil {
		return err
	}

	rootDir := DeserializeEntries(bytes.NewBuffer(rootBytes), header.InternalCompression)

	tileEntries, leaves := RelevantEntries(relevantSet, uint8(maxzoom), rootDir)

	// 4. get all relevant leaf entries

	leafRanges := make([]srcDstRange, 0)
	for _, leaf := range leaves {
		leafRanges = append(leafRanges, srcDstRange{header.LeafDirectoryOffset + leaf.Offset, 0, uint64(leaf.Length)})
	}

	overfetchLeaves, _ := mergeRanges(leafRanges, overfetch)
	numOverfetchLeaves := overfetchLeaves.Len()
	logger.Printf("fetching %d dirs, %d chunks, %d requests\n", len(leaves), len(leafRanges), overfetchLeaves.Len())

	for {
		if overfetchLeaves.Len() == 0 {
			break
		}
		or := overfetchLeaves.Remove(overfetchLeaves.Front()).(overfetchRange)

		chunkReader, err := readSource(int64(or.Rng.SrcOffset), int64(or.Rng.Length))
		if err != nil {
			return err
		}

		for _, cd := range or.CopyDiscards {

			leafBytes := make([]byte, cd.Wanted)
			_, err := io.ReadFull(chunkReader, leafBytes)
			if err != nil {
				return err
			}
			leafdir := DeserializeEntries(bytes.NewBuffer(leafBytes), header.InternalCompression)
			newEntries, newLeaves := RelevantEntries(relevantSet, uint8(maxzoom), leafdir)

			if len(newLeaves) > 0 {
				panic("This doesn't support leaf level 2+.")
			}
			tileEntries = append(tileEntries, newEntries...)

			_, err = io.CopyN(io.Discard, chunkReader, int64(cd.Discard))
			if err != nil {
				return err
			}
		}
		chunkReader.Close()
	}

	sort.Slice(tileEntries, func(i, j int) bool {
		return tileEntries[i].TileID < tileEntries[j].TileID
	})

	logger.Printf("Region tiles %d, result tile entries %d\n", relevantSet.GetCardinality(), len(tileEntries))

	// 6. create the new header and chunk list
	// we now need to re-encode this entry list using cumulative offsets
	reencoded, tileParts, tiledataLength, addressedTiles, tileContents := reencodeEntries(tileEntries)

	overfetchRanges, totalBytes := mergeRanges(tileParts, overfetch)

	numOverfetchRanges := overfetchRanges.Len()
	logger.Printf("fetching %d tiles, %d chunks, %d requests\n", len(reencoded), len(tileParts), overfetchRanges.Len())

	// TODO: takes up too much RAM
	// construct the directories
	newRootBytes, newLeavesBytes, _ := BuildDirectories(reencoded, 16384-HeaderV3LenBytes, Gzip)

	// 7. write the modified header
	header.RootOffset = HeaderV3LenBytes
	header.RootLength = uint64(len(newRootBytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(newLeavesBytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength

	header.TileDataLength = tiledataLength
	header.AddressedTilesCount = addressedTiles
	header.TileEntriesCount = uint64(len(tileEntries))
	header.TileContentsCount = tileContents

	header.MaxZoom = uint8(maxzoom)
	header.MinZoom = uint8(minzoom)
	if header.CenterZoom < header.MinZoom {
		header.CenterZoom = header.MinZoom
	} else if header.CenterZoom > header.MaxZoom {
		header.CenterZoom = header.MaxZoom
	}

	headerBytes := SerializeHeader(header)

	totalActualBytes := uint64(0)
	for _, x := range tileParts {
		totalActualBytes += x.Length
	}

	if !dryRun {
		outputSize := HeaderV3LenBytes + int64(len(newRootBytes)) + int64(header.MetadataLength) + int64(len(newLeavesBytes)) + int64(totalActualBytes)

		// With resume turned on, state holds the progress that the resume state file records.
		var state extractResumeState
		resuming := false
		if resume.StatePath != "" {
			regionHash, err := regionSHA256(regionFile)
			if err != nil {
				return err
			}
			identity := extractResumeState{
				FormatVersion: extractResumeStateFormatVersion,
				SourceURL:     bucketURL + "/" + key,
				SourceEtag:    sourceEtag,
				MinZoom:       minzoom,
				MaxZoom:       maxzoom,
				Bbox:          bbox,
				RegionSHA256:  regionHash,
				Overfetch:     overfetch,
				PlanSHA256:    extractPlanSHA256(headerBytes, newRootBytes, newLeavesBytes, overfetchRanges),
			}
			var complete bool
			state, resuming, complete, err = prepareExtractResume(resume, output, identity, outputSize)
			if err != nil {
				return err
			}
			if complete {
				logger.Printf("%s is complete, according to %s.\n", output, resume.StatePath)
				return nil
			}
		}

		var outfile *os.File
		if resuming {
			logger.Printf("Resuming %s from %s.\n", output, resume.StatePath)
			outfile, err = os.OpenFile(output, os.O_RDWR, 0)
		} else {
			outfile, err = os.Create(output)
		}
		if err != nil {
			return err
		}
		defer outfile.Close()

		// set the file size and write empty space for the header for now
		// see comment below
		if !resuming {
			if err := outfile.Truncate(outputSize); err != nil {
				return err
			}
			// Record the new output with no progress, so that a failure before the
			// first checkpoint leaves an output that a later run can resume.
			if resume.StatePath != "" {
				if err := writeExtractResumeState(resume.StatePath, state); err != nil {
					return err
				}
			}
		}
		_, err = outfile.Write(make([]byte, HeaderV3LenBytes))
		if err != nil {
			return err
		}

		// 8. write the root directory
		_, err = outfile.Write(newRootBytes)
		if err != nil {
			return err
		}

		// 9. get and write the metadata
		metadataReader, err := readSource(int64(sourceMetadataOffset), int64(header.MetadataLength))
		if err != nil {
			return err
		}
		metadataBytes, err := io.ReadAll(metadataReader)
		defer metadataReader.Close()
		if err != nil {
			return err
		}

		_, err = outfile.Write(metadataBytes)
		if err != nil {
			return err
		}

		// 10. write the leaf directories
		_, err = outfile.Write(newLeavesBytes)
		if err != nil {
			return err
		}

		bar := defaultBytesProgressbar(
			logger,
			int64(totalBytes),
			"fetching chunks",
		)

		var mu sync.Mutex

		// rangeBytesDone holds, for each download range by its source offset,
		// the number of source bytes from the start of the range that are written to the output.
		var progressMu sync.Mutex
		rangeBytesDone := make(map[uint64]uint64)
		for srcOffset, done := range state.RangeBytesDone {
			rangeBytesDone[srcOffset] = done
			bar.Add64(int64(done))
		}

		// checkpoint syncs the output, then records the progress from before the sync
		// in the resume state file. Every byte that the file records is then on disk.
		var checkpointMu sync.Mutex
		checkpoint := func() error {
			if resume.StatePath == "" {
				return nil
			}
			checkpointMu.Lock()
			defer checkpointMu.Unlock()
			progressMu.Lock()
			snapshot := make(map[uint64]uint64, len(rangeBytesDone))
			for srcOffset, done := range rangeBytesDone {
				snapshot[srcOffset] = done
			}
			progressMu.Unlock()
			if err := outfile.Sync(); err != nil {
				return err
			}
			state.RangeBytesDone = snapshot
			return writeExtractResumeState(resume.StatePath, state)
		}

		// The first checkpoint records the directories as written, and creates the resume state file.
		if err := checkpoint(); err != nil {
			return err
		}

		downloadPart := func(or overfetchRange) error {
			srcOffset := or.Rng.SrcOffset
			progressMu.Lock()
			done := rangeBytesDone[srcOffset]
			progressMu.Unlock()
			if done >= or.Rng.Length {
				return nil
			}

			tileReader, err := readSource(int64(sourceTileDataOffset+srcOffset+done), int64(or.Rng.Length-done))
			if err != nil {
				return err
			}
			defer tileReader.Close()

			// position counts the source bytes from the start of the range.
			// dstPosition is the offset in the output of the next wanted byte.
			position := uint64(0)
			dstPosition := int64(header.TileDataOffset + or.Rng.DstOffset)
			buffer := make([]byte, 1<<20)

			for _, cd := range or.CopyDiscards {
				for _, part := range []struct {
					length uint64
					wanted bool
				}{{cd.Wanted, true}, {cd.Discard, false}} {
					end := position + part.length
					// Skip the bytes that an earlier run wrote.
					if position < done {
						skipped := min(end, done) - position
						if part.wanted {
							dstPosition += int64(skipped)
						}
						position += skipped
					}
					for position < end {
						n, readErr := tileReader.Read(buffer[:min(uint64(len(buffer)), end-position)])
						if n > 0 {
							if part.wanted {
								if _, err := outfile.WriteAt(buffer[:n], dstPosition); err != nil {
									return err
								}
								dstPosition += int64(n)
							}
							position += uint64(n)
							bar.Add(n)
							progressMu.Lock()
							rangeBytesDone[srcOffset] = position
							progressMu.Unlock()
						}
						if readErr == io.EOF && position < end {
							return io.ErrUnexpectedEOF
						}
						if readErr != nil && readErr != io.EOF {
							return readErr
						}
					}
				}
			}
			return nil
		}

		// With resume turned on, a checkpoint runs at each interval while the download runs.
		stopCheckpoints := make(chan struct{})
		checkpointsStopped := make(chan struct{})
		go func() {
			defer close(checkpointsStopped)
			if resume.StatePath == "" {
				return
			}
			ticker := time.NewTicker(extractResumeCheckpointInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err := checkpoint(); err != nil {
						logger.Printf("Failed to save the resume state: %v\n", err)
					}
				case <-stopCheckpoints:
					return
				}
			}
		}()

		errs, _ := errgroup.WithContext(ctx)

		for i := 0; i < downloadThreads; i++ {
			workBack := (i == 0 && downloadThreads > 1)
			errs.Go(func() error {
				done := false
				var or overfetchRange
				for {
					mu.Lock()
					if overfetchRanges.Len() == 0 {
						done = true
					} else {
						if workBack {
							or = overfetchRanges.Remove(overfetchRanges.Back()).(overfetchRange)
						} else {
							or = overfetchRanges.Remove(overfetchRanges.Front()).(overfetchRange)
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
			})
		}

		err = errs.Wait()
		close(stopCheckpoints)
		<-checkpointsStopped
		if err != nil {
			// Record the progress, so that a resumed extract continues from it.
			if checkpointErr := checkpoint(); checkpointErr != nil {
				logger.Printf("Failed to save the resume state: %v\n", checkpointErr)
			}
			return err
		}

		// Write the header when finishing,
		// otherwise a extract cancelled during the tile download section
		// will appear valid with "pmtiles verify"
		// even though the tile contents are corrupted.
		// Sync before the header write, so the tile data is on disk before the header,
		// and after it, so the header is on disk when Extract returns.
		if err := outfile.Sync(); err != nil {
			return err
		}
		_, err = outfile.WriteAt(headerBytes, 0)
		if err != nil {
			return err
		}
		if err := outfile.Sync(); err != nil {
			return err
		}

		if resume.StatePath != "" {
			if resume.KeepState {
				state.Complete = true
				if err := checkpoint(); err != nil {
					return err
				}
			} else if err := os.Remove(resume.StatePath); err != nil {
				return err
			}
		}
	}

	logger.Printf("Completed in %v with %v download threads (%v tiles/s).\n", time.Since(start), downloadThreads, float64(len(reencoded))/float64(time.Since(start).Seconds()))
	totalRequests := 2                  // header + root
	totalRequests += numOverfetchLeaves // leaves
	totalRequests++                     // metadata
	totalRequests += numOverfetchRanges
	logger.Printf("Extract required %d total requests.\n", totalRequests)
	logger.Printf("Extract transferred %s (overfetch %v) for an archive size of %s\n", humanize.Bytes(totalBytes), overfetch, humanize.Bytes(totalActualBytes))

	return nil
}
