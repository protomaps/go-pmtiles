package main

import (
	"bytes"
	"container/list"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Key struct {
	name string
	rng  pmtiles.Range
}

type DatumKind int

const (
	Root DatumKind = iota
	Tile
	Leaf
)

type Datum struct {
	bytes     []byte
	directory pmtiles.Directory
	kind      DatumKind
}

type Request struct {
	key   Key
	kind  DatumKind
	value chan Datum
}

type Response struct {
	key   Key
	value Datum
	size  int
}

type Fetcher interface {
	Do(key Key, readFunc func(io.Reader))
}

type HTTPFetcher struct {
	bucket string
	client *http.Client
}

func (fetcher HTTPFetcher) Do(key Key, readFunc func(io.Reader)) {
	archive := fetcher.bucket + "/" + key.name + ".pmtiles"
	fetch, _ := http.NewRequest("GET", archive, nil)
	end := key.rng.Offset + uint64(key.rng.Length) - 1
	range_header := fmt.Sprintf("bytes=%d-%d", key.rng.Offset, end)
	fetch.Header.Add("Range", range_header)
	fetch_resp, _ := fetcher.client.Do(fetch)
	defer fetch_resp.Body.Close()
	readFunc(fetch_resp.Body)
}

type FileFetcher struct {
	path string
}

func (fetcher FileFetcher) Do(key Key, readFunc func(io.Reader)) {
	f, _ := os.Open(fetcher.path + "/" + key.name + ".pmtiles")
	f.Seek(int64(key.rng.Offset), 0)
	defer f.Close()
	readFunc(f)
}

func main() {
	port := flag.String("p", "8077", "port to serve on")
	var cors string
	var cacheSize int
	flag.StringVar(&cors, "cors", "", "CORS allowed origin value")
	flag.IntVar(&cacheSize, "cache", 64, "Cache size in mb")
	flag.Parse()
	path := flag.Arg(0)

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	if path == "" {
		logger.Println("USAGE: go-pmtiles LOCAL_PATH or https://BUCKET")
		os.Exit(1)
	}

	var fetcher Fetcher
	if strings.HasPrefix(path, "http") {
		fetcher = HTTPFetcher{client: &http.Client{}, bucket: path}
	} else {
		fetcher = FileFetcher{path: path}
	}

	reqs := make(chan Request, 8)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rPath := regexp.MustCompile(`\/(?P<NAME>[A-Za-z_]+)\/(?P<Z>\d+)\/(?P<X>\d+)\/(?P<Y>\d+)\.(?P<EXT>png|pbf|jpg)`)
		res := rPath.FindStringSubmatch(r.URL.Path)
		if len(res) == 0 {
			mPath := regexp.MustCompile(`\/(?P<NAME>[A-Za-z_]+)\/metadata`)
			res = mPath.FindStringSubmatch(r.URL.Path)

			if len(res) == 0 {
				w.WriteHeader(404)
				return
			}

			name := res[1]
			root_req := Request{kind: Root, key: Key{name: name, rng: pmtiles.Range{Offset: 0, Length: 512000}}, value: make(chan Datum, 1)}
			reqs <- root_req
			root_value := <-root_req.value
			if len(cors) > 0 {
				w.Header().Set("Access-Control-Allow-Origin", cors)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(root_value.bytes)
			return
		}

		name := res[1]

		root_req := Request{kind: Root, key: Key{name: name, rng: pmtiles.Range{Offset: 0, Length: 512000}}, value: make(chan Datum, 1)}
		reqs <- root_req

		// https://golang.org/doc/faq#atomic_maps
		root_value := <-root_req.value

		z, _ := strconv.ParseUint(res[2], 10, 8)
		x, _ := strconv.ParseUint(res[3], 10, 32)
		y, _ := strconv.ParseUint(res[4], 10, 32)
		coord := pmtiles.Zxy{Z: uint8(z), X: uint32(x), Y: uint32(y)}

		var tile []byte
		if offsetlen, ok := root_value.directory.Entries[coord]; ok {
			tile_req := Request{kind: Tile, key: Key{name: name, rng: offsetlen}, value: make(chan Datum, 1)}
			reqs <- tile_req
			tile_value := <-tile_req.value
			tile = tile_value.bytes
		} else {
			leaf := pmtiles.GetParentTile(coord, root_value.directory.LeafZ)

			offsetlen := root_value.directory.Leaves[leaf]
			leaf_req := Request{kind: Leaf, key: Key{name: name, rng: offsetlen}, value: make(chan Datum, 1)}
			reqs <- leaf_req
			leaf_value := <-leaf_req.value

			offsetlen = leaf_value.directory.Entries[coord]
			tile_req := Request{kind: Tile, key: Key{name: name, rng: offsetlen}, value: make(chan Datum, 1)}
			reqs <- tile_req
			tile_value := <-tile_req.value
			tile = tile_value.bytes
		}

		ext := res[5]
		var content_type string
		switch ext {
		case "jpg":
			content_type = "image/jpeg"
		case "png":
			content_type = "image/png"
		case "pbf":
			content_type = "application/x-protobuf"
		}
		w.Header().Set("Content-Type", content_type)
		if len(cors) > 0 {
			w.Header().Set("Access-Control-Allow-Origin", cors)
		}

		if ext == "pbf" {
			w.Header().Set("Content-Encoding", "gzip")
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			_, _ = zw.Write(tile)
			zw.Close()
			w.Write(buf.Bytes())
		} else {
			w.Write(tile)
		}
		elapsed := time.Since(start)
		logger.Printf("served %s/%d/%d/%d in %s", name, z, x, y, elapsed)
	})

	go func() {
		cache := make(map[Key]*list.Element)
		inflight := make(map[Key][]Request)
		resps := make(chan Response, 8)
		evictList := list.New()
		totalSize := 0

		for {
			select {
			case req := <-reqs:
				key := req.key
				if val, ok := cache[key]; ok {
					evictList.MoveToFront(val)
					req.value <- val.Value.(*Response).value
				} else if _, ok := inflight[key]; ok {
					inflight[key] = append(inflight[key], req)
				} else {
					inflight[key] = []Request{req}
					go func() {
						var result Datum
						var size int
						fetcher.Do(key, func(reader io.Reader) {
							if req.kind == Root {
								metadata, dir := pmtiles.ParseHeader(reader)
								result = Datum{kind: Root, bytes: metadata, directory: dir}
								size = len(metadata) + dir.SizeBytes()
							} else if req.kind == Leaf {
								dir := pmtiles.ParseDirectory(reader, key.rng.Length/17)
								result = Datum{kind: Root, directory: dir}
								size = dir.SizeBytes()
							} else {
								tile := make([]byte, key.rng.Length)
								_, _ = io.ReadFull(reader, tile)
								result = Datum{kind: Tile, bytes: tile}
								size = len(tile)
							}
						})
						resps <- Response{key: key, value: result, size: size}
						logger.Printf("fetched %s %d-%d", key.name, key.rng.Offset, key.rng.Length)
					}()
				}
			case resp := <-resps:
				key := resp.key
				for _, v := range inflight[key] {
					v.value <- resp.value
				}
				totalSize += resp.size
				ent := &resp
				entry := evictList.PushFront(ent)
				cache[key] = entry

				delete(inflight, key)

				for {
					if totalSize < cacheSize*1000*1000 {
						break
					}
					ent := evictList.Back()
					if ent != nil {
						evictList.Remove(ent)
						kv := ent.Value.(*Response)
						delete(cache, kv.key)
						totalSize -= kv.size
					}
				}
			}
		}
	}()

	logger.Printf("Serving %s on HTTP port: %s\n",path, *port)
	logger.Fatal(http.ListenAndServe(":"+*port, nil))
}
