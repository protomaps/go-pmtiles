package pmtiles

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type multiRange struct {
	str    string
	ranges []srcDstRange
}

func makeMultiRanges(ranges []srcDstRange, baseOffset int64, maxHeaderBytes int) []multiRange {
	var result []multiRange
	var b strings.Builder
	var currentRanges []srcDstRange

	for _, r := range ranges {
		rangeStr := fmt.Sprintf("%d-%d",
			baseOffset+int64(r.SrcOffset),
			baseOffset+int64(r.SrcOffset)+int64(r.Length)-1,
		)

		if b.Len()+len(rangeStr)+1 > maxHeaderBytes {
			if b.Len() > 0 {
				result = append(result, multiRange{
					str:    b.String(),
					ranges: currentRanges,
				})
				b.Reset()
				currentRanges = nil
			}
		}

		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(rangeStr)
		currentRanges = append(currentRanges, r)
	}

	if b.Len() > 0 {
		result = append(result, multiRange{
			str:    b.String(),
			ranges: currentRanges,
		})
	}

	return result
}

func Sync(logger *log.Logger, oldVersion string, newVersion string, dryRun bool) error {
	fmt.Println("WARNING: This is an experimental feature. Do not rely on this in production!")
	start := time.Now()

	client := &http.Client{}

	var bufferedReader *bufio.Reader
	req, err := http.NewRequest("GET", newVersion+".sync", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(".sync file not found")
	}
	if err != nil {
		return err
	}
	bar := progressbar.DefaultBytes(
		resp.ContentLength,
		"downloading syncfile",
	)
	bufferedReader = bufio.NewReader(io.TeeReader(resp.Body, bar))

	var syncHeader syncHeader
	jsonBytes, _ := bufferedReader.ReadSlice('\n')

	json.Unmarshal(jsonBytes, &syncHeader)

	blocks := deserializeSyncBlocks(syncHeader.NumBlocks, bufferedReader)
	bar.Close()

	ctx := context.Background()

	oldFile, err := os.OpenFile(oldVersion, os.O_RDONLY, 0666)
	defer oldFile.Close()

	if err != nil {
		return err
	}

	buf := make([]byte, HeaderV3LenBytes)
	_, err = oldFile.Read(buf)
	if err != nil {
		return err
	}
	oldHeader, err := DeserializeHeader(buf)
	if err != nil {
		return err
	}

	if !oldHeader.Clustered {
		return fmt.Errorf("archive must be clustered for sync")
	}

	bar = progressbar.Default(
		int64(len(blocks)),
		"calculating diff",
	)

	wanted := make([]syncBlock, 0)
	have := make([]srcDstRange, 0)

	idx := 0

	tasks := make(chan syncTask, 1000)
	var wg sync.WaitGroup
	var mu sync.Mutex

	errs, _ := errgroup.WithContext(ctx)

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		errs.Go(func() error {
			wg.Add(1)
			for task := range tasks {
				hasher := xxhash.New()
				r := io.NewSectionReader(oldFile, int64(oldHeader.TileDataOffset+task.OldOffset), int64(task.NewBlock.Length))

				if _, err := io.Copy(hasher, r); err != nil {
					log.Fatal(err)
				}

				mu.Lock()
				if task.NewBlock.Hash == hasher.Sum64() {
					have = append(have, srcDstRange{SrcOffset: task.OldOffset, DstOffset: task.NewBlock.Offset, Length: task.NewBlock.Length})
				} else {
					wanted = append(wanted, task.NewBlock)
				}
				mu.Unlock()
			}
			wg.Done()
			return nil
		})
	}

	err = IterateEntries(oldHeader,
		func(offset uint64, length uint64) ([]byte, error) {
			return io.ReadAll(io.NewSectionReader(oldFile, int64(offset), int64(length)))
		},
		func(e EntryV3) {
			if idx < len(blocks) {
				for e.TileID > blocks[idx].Start {
					mu.Lock()
					wanted = append(wanted, blocks[idx])
					mu.Unlock()
					bar.Add(1)
					idx = idx + 1
				}

				if e.TileID == blocks[idx].Start {
					tasks <- syncTask{NewBlock: blocks[idx], OldOffset: e.Offset}
					bar.Add(1)
					idx = idx + 1
				}
			}
		})

	if err != nil {
		return err
	}

	// we may not've consumed until the end
	for idx < len(blocks) {
		mu.Lock()
		wanted = append(wanted, blocks[idx])
		mu.Unlock()
		bar.Add(1)
		idx = idx + 1
	}

	close(tasks)
	wg.Wait()

	sort.Slice(wanted, func(i, j int) bool { return wanted[i].Start < wanted[j].Start })
	sort.Slice(have, func(i, j int) bool { return have[i].SrcOffset < have[j].SrcOffset })

	toTransfer := uint64(0)
	totalRemoteBytes := uint64(0)
	for _, v := range wanted {
		toTransfer += v.Length
		totalRemoteBytes += v.Length
	}

	for _, v := range have {
		totalRemoteBytes += v.Length
	}

	blocksMatched := float64(len(have)) / float64(len(blocks)) * 100
	pct := float64(toTransfer) / float64(totalRemoteBytes) * 100

	fmt.Printf("%d/%d blocks matched (%.1f%%), need to transfer %s/%s (%.1f%%).\n", len(have), len(blocks), blocksMatched, humanize.Bytes(toTransfer), humanize.Bytes(totalRemoteBytes), pct)

	ranges := make([]srcDstRange, 0)
	for _, v := range wanted {
		l := len(ranges)
		// combine contiguous ranges
		if l > 0 && (ranges[l-1].SrcOffset+ranges[l-1].Length) == v.Offset {
			ranges[l-1].Length = ranges[l-1].Length + v.Length
		} else {
			ranges = append(ranges, srcDstRange{SrcOffset: v.Offset, DstOffset: v.Offset, Length: v.Length})
		}
	}

	haveRanges := make([]srcDstRange, 0)
	for _, v := range have {
		l := len(haveRanges)
		// combine contiguous ranges
		if l > 0 && (haveRanges[l-1].SrcOffset+haveRanges[l-1].Length) == v.SrcOffset {
			haveRanges[l-1].Length = haveRanges[l-1].Length + v.Length
		} else {
			haveRanges = append(haveRanges, v)
		}
	}

	fmt.Printf("need %d chunks\n", len(ranges))

	if !dryRun {
		req, err := http.NewRequest("HEAD", newVersion, nil)
		resp, err := client.Do(req)
		targetLength, _ := strconv.Atoi(resp.Header.Get("Content-Length"))

		tmpFilename := oldVersion + ".tmp"
		outfile, err := os.Create(tmpFilename)
		outfile.Truncate(int64(targetLength))

		// write the first 16 kb to the new file
		req, err = http.NewRequest("GET", newVersion, nil)
		req.Header.Set("Range", "bytes=0-16383")
		resp, err = client.Do(req)
		bufferedReader = bufio.NewReader(io.TeeReader(resp.Body, outfile))
		if err != nil {
			return err
		}
		bytesData, err := io.ReadAll(bufferedReader)
		if err != nil {
			return err
		}
		newHeader, err := DeserializeHeader(bytesData[0:HeaderV3LenBytes])
		if err != nil {
			return err
		}

		// write the metadata section to the new file
		metadataWriter := io.NewOffsetWriter(outfile, int64(newHeader.MetadataOffset))
		req, err = http.NewRequest("GET", newVersion, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", newHeader.MetadataOffset, newHeader.MetadataOffset+newHeader.MetadataLength-1))
		resp, err = client.Do(req)
		io.Copy(metadataWriter, resp.Body)

		// write the leaf directories, if any, to the new file (show progress)
		leafWriter := io.NewOffsetWriter(outfile, int64(newHeader.LeafDirectoryOffset))
		req, err = http.NewRequest("GET", newVersion, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", newHeader.LeafDirectoryOffset, newHeader.LeafDirectoryOffset+newHeader.LeafDirectoryLength-1))
		resp, err = client.Do(req)

		leafBar := progressbar.DefaultBytes(
			int64(newHeader.LeafDirectoryLength),
			"downloading leaf directories",
		)
		io.Copy(leafWriter, io.TeeReader(resp.Body, leafBar))
		leafBar.Close()

		fmt.Println(len(have), "local chunks")
		bar := progressbar.DefaultBytes(
			int64(totalRemoteBytes-toTransfer),
			"copying local chunks",
		)

		// write the tile data (from local)
		for _, h := range haveRanges {
			chunkWriter := io.NewOffsetWriter(outfile, int64(newHeader.TileDataOffset+h.DstOffset))
			r := io.NewSectionReader(oldFile, int64(oldHeader.TileDataOffset+h.SrcOffset), int64(h.Length))
			io.Copy(io.MultiWriter(chunkWriter, bar), r)
		}

		oldFile.Close()

		// write the tile data (from remote)
		multiRanges := makeMultiRanges(ranges, int64(newHeader.TileDataOffset), 1048576-200)
		fmt.Println("Batched into http requests", len(multiRanges))

		bar = progressbar.DefaultBytes(
			int64(toTransfer),
			"fetching remote chunks",
		)

		downloadPart := func(task multiRange) error {
			req, err := http.NewRequest("GET", newVersion, nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%s", task.str))
			resp, err := client.Do(req)
			if resp.StatusCode != http.StatusPartialContent {
				return fmt.Errorf("non-OK multirange request")
			}

			_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
			if err != nil {
				return err
			}

			mr := multipart.NewReader(resp.Body, params["boundary"])

			for _, r := range task.ranges {
				part, _ := mr.NextPart()
				_ = part.Header.Get("Content-Range")
				chunkWriter := io.NewOffsetWriter(outfile, int64(newHeader.TileDataOffset+r.DstOffset))
				io.Copy(io.MultiWriter(chunkWriter, bar), part)
			}
			return nil
		}

		var mu sync.Mutex
		downloadThreads := 4

		errs, _ := errgroup.WithContext(ctx)

		for i := 0; i < downloadThreads; i++ {
			errs.Go(func() error {
				done := false
				var head multiRange
				for {
					mu.Lock()
					if len(multiRanges) == 0 {
						done = true
					} else {
						head, multiRanges = multiRanges[0], multiRanges[1:]
					}
					mu.Unlock()
					if done {
						return nil
					}
					err := downloadPart(head)
					if err != nil {
						return err
					}
				}
			})
		}

		err = errs.Wait()
		if err != nil {
			return err
		}

		// atomically rename the old file to the new file.
		outfile.Close()
		err = os.Rename(tmpFilename, oldVersion)
		if err != nil {
			return err
		}
	}

	fmt.Printf("Completed sync in %v.\n", time.Since(start))
	return nil
}
