package pmtiles

import (
	"bufio"
	"bytes"
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
	str string
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

func Sync(logger *log.Logger, oldVersion string, newVersion string, newFile string, dryRun bool) error {
	start := time.Now()

	client := &http.Client{}

	var bufferedReader *bufio.Reader
	if strings.HasPrefix(newVersion, "http") {
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
		bar.Close()
	} else {
		newFile, err := os.Open(newVersion + ".sync")
		if err != nil {
			return fmt.Errorf("error opening syncfile: %v", err)
		}
		defer newFile.Close()
		bufferedReader = bufio.NewReader(newFile)
	}

	var syncHeader syncHeader
	jsonBytes, _ := bufferedReader.ReadSlice('\n')

	json.Unmarshal(jsonBytes, &syncHeader)

	if len(syncHeader.B3Sum) > 0 {
		fmt.Println("b3sum", syncHeader.B3Sum)
	}

	blocks := deserializeSyncBlocks(syncHeader.NumBlocks, bufferedReader)

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
	oldHeader, err := deserializeHeader(buf)
	if err != nil {
		return err
	}

	if !oldHeader.Clustered {
		return fmt.Errorf("archive must be clustered for sync")
	}

	var CollectEntries func(uint64, uint64, func(EntryV3))

	CollectEntries = func(dir_offset uint64, dir_length uint64, f func(EntryV3)) {
		dirbytes := io.NewSectionReader(oldFile, int64(dir_offset), int64(dir_length))

		b, err := io.ReadAll(dirbytes)
		if err != nil {
			panic(fmt.Errorf("I/O Error"))
		}

		directory := deserializeEntries(bytes.NewBuffer(b))
		for _, entry := range directory {
			if entry.RunLength > 0 {
				f(entry)
			} else {
				CollectEntries(oldHeader.LeafDirectoryOffset+entry.Offset, uint64(entry.Length), f)
			}
		}
	}

	bar := progressbar.Default(
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
				r := io.NewSectionReader(oldFile, int64(oldHeader.TileDataOffset + task.OldOffset), int64(task.NewBlock.Length))

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

	CollectEntries(oldHeader.RootOffset, oldHeader.RootLength, func(e EntryV3) {
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

		outfile, err := os.Create(newFile)
		defer outfile.Close()
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
		newHeader, err := deserializeHeader(bytesData[0:HeaderV3LenBytes])
		if err != nil {
			return err
		}

		// write the metadata section to the new file
		metadataWriter := io.NewOffsetWriter(outfile, int64(newHeader.MetadataOffset))
		req, err = http.NewRequest("GET", newVersion, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", newHeader.MetadataOffset, newHeader.MetadataOffset+newHeader.MetadataLength-1))
		resp, err = client.Do(req)
		io.Copy(metadataWriter, resp.Body)

		// write the leaf directories, if any, to the new file
		leafWriter := io.NewOffsetWriter(outfile, int64(newHeader.LeafDirectoryOffset))
		req, err = http.NewRequest("GET", newVersion, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", newHeader.LeafDirectoryOffset, newHeader.LeafDirectoryOffset+newHeader.LeafDirectoryLength-1))
		resp, err = client.Do(req)
		io.Copy(leafWriter, resp.Body)

		fmt.Println(len(have), "local chunks")
		bar := progressbar.DefaultBytes(
			int64(totalRemoteBytes-toTransfer),
			"copying local chunks",
		)

		// write the tile data (from local)
		for _, h := range haveRanges {
			chunkWriter := io.NewOffsetWriter(outfile, int64(newHeader.TileDataOffset+h.DstOffset))
			r := io.NewSectionReader(oldFile, int64(oldHeader.TileDataOffset + h.SrcOffset), int64(h.Length))
			io.Copy(io.MultiWriter(chunkWriter, bar), r)
		}

		// write the tile data (from remote)
		multiRanges := makeMultiRanges(ranges, int64(newHeader.TileDataOffset), 1048576 - 200)
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
	}

	fmt.Printf("Completed sync in %v.\n", time.Since(start))
	return nil
}
