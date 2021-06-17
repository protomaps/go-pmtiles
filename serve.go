package main

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"flag"
	"fmt"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"io"
	"log"
	"net/http"
	"os"
	"path"
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
	hit       bool
}

type Request struct {
	key   Key
	kind  DatumKind
	value chan Datum
}

type Response struct {
	key    Key
	value  Datum
	size   int
	ok     bool
	misses int
}

type Fetcher interface {
	Do(key Key, readFunc func(io.Reader)) bool
}

type HTTPFetcher struct {
	bucket string
	client *http.Client
}

func (fetcher HTTPFetcher) Do(key Key, readFunc func(io.Reader)) bool {
	archive := fetcher.bucket + "/" + key.name + ".pmtiles"
	fetch, _ := http.NewRequest("GET", archive, nil)
	end := key.rng.Offset + uint64(key.rng.Length) - 1
	range_header := fmt.Sprintf("bytes=%d-%d", key.rng.Offset, end)
	fetch.Header.Add("Range", range_header)
	fetch_resp, err := fetcher.client.Do(fetch)
	if err != nil || fetch_resp.StatusCode >= 300 || fetch_resp.ContentLength != int64(key.rng.Length) {
		return false
	}
	defer fetch_resp.Body.Close()
	readFunc(fetch_resp.Body)
	return true
}

type FileFetcher struct {
	path string
}

type Loop struct {
	reqs      chan Request
	fetcher   Fetcher
	logger    *log.Logger
	cacheSize int
	cors	  string
}

func NewLoop(path string, logger *log.Logger, cacheSize int, cors string) Loop {
	reqs := make(chan Request, 8)
	var fetcher Fetcher
	if strings.HasPrefix(path, "http") {
		path = strings.TrimSuffix(path, "/")
		fetcher = HTTPFetcher{client: &http.Client{}, bucket: path}
	} else {
		fetcher = FileFetcher{path: path}
	}
	return Loop{reqs: reqs, fetcher: fetcher, logger: logger, cacheSize: cacheSize, cors: cors}
}

func (loop Loop) Start() {
	go func() {
		cache := make(map[Key]*list.Element)
		inflight := make(map[Key][]Request)
		resps := make(chan Response, 8)
		evictList := list.New()
		totalSize := 0

		for {
			select {
			case req := <-loop.reqs:
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
						ok := loop.fetcher.Do(key, func(reader io.Reader) {
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
						resps <- Response{key: key, value: result, size: size, ok: ok}
						if ok {
							loop.logger.Printf("fetched %s %d-%d", key.name, key.rng.Offset, key.rng.Length)
						} else {
							loop.logger.Printf("failed to fetch %s %d-%d", key.name, key.rng.Offset, key.rng.Length)
						}
					}()
				}
			case resp := <-resps:
				key := resp.key
				resp.value.hit = false
				for _, v := range inflight[key] {
					v.value <- resp.value
				}
				delete(inflight, key)

				if resp.ok {
					resp.value.hit = true
					totalSize += resp.size
					ent := &resp
					entry := evictList.PushFront(ent)
					cache[key] = entry

					for {
						if totalSize < loop.cacheSize*1000*1000 {
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
		}
	}()
}

func (loop Loop) Get(path string) (int,map[string]string,[]byte) {
	headers := make(map[string]string)
	if len(loop.cors) > 0 {
		headers["Access-Control-Allow-Origin"] = loop.cors
	}
	start := time.Now()
	rPath := regexp.MustCompile(`\/(?P<NAME>[A-Za-z0-9_]+)\/(?P<Z>\d+)\/(?P<X>\d+)\/(?P<Y>\d+)\.(?P<EXT>png|pbf|jpg)`)
	res := rPath.FindStringSubmatch(path)
	misses := 0

	if len(res) == 0 {
		mPath := regexp.MustCompile(`\/(?P<NAME>[A-Za-z0-9_]+)\/metadata`)
		res = mPath.FindStringSubmatch(path)

		if len(res) == 0 {
			return 404, headers, nil
		}

		name := res[1]
		root_req := Request{kind: Root, key: Key{name: name, rng: pmtiles.Range{Offset: 0, Length: 512000}}, value: make(chan Datum, 1)}
		loop.reqs <- root_req
		root_value := <-root_req.value
		if !root_value.hit {
			misses++
		}
		headers["Content-Length"] = strconv.Itoa(len(root_value.bytes))
		headers["Content-Type"] = "application/json"
		headers["Pmap-Cache-Misses"] = strconv.Itoa(misses)
		return 200, headers, root_value.bytes
	}

	name := res[1]

	root_req := Request{kind: Root, key: Key{name: name, rng: pmtiles.Range{Offset: 0, Length: 512000}}, value: make(chan Datum, 1)}
	loop.reqs <- root_req

	// https://golang.org/doc/faq#atomic_maps
	root_value := <-root_req.value
	if !root_value.hit {
		misses++
	}

	z, _ := strconv.ParseUint(res[2], 10, 8)
	x, _ := strconv.ParseUint(res[3], 10, 32)
	y, _ := strconv.ParseUint(res[4], 10, 32)
	coord := pmtiles.Zxy{Z: uint8(z), X: uint32(x), Y: uint32(y)}

	if len(root_value.directory.Entries) == 0 {
		return 404, headers, nil
	}

	var tile []byte
	if offsetlen, ok := root_value.directory.Entries[coord]; ok {
		tile_req := Request{kind: Tile, key: Key{name: name, rng: offsetlen}, value: make(chan Datum, 1)}
		loop.reqs <- tile_req
		tile_value := <-tile_req.value
		if !tile_value.hit {
			misses++
		}
		tile = tile_value.bytes
	} else {
		if coord.Z < root_value.directory.LeafZ {
			return 404, headers, nil
		}
		leaf := pmtiles.GetParentTile(coord, root_value.directory.LeafZ)

		offsetlen, ok := root_value.directory.Leaves[leaf]
		if !ok {
			return 404, headers, nil
		}
		leaf_req := Request{kind: Leaf, key: Key{name: name, rng: offsetlen}, value: make(chan Datum, 1)}
		loop.reqs <- leaf_req
		leaf_value := <-leaf_req.value
		if !leaf_value.hit {
			misses++
		}

		offsetlen, ok = leaf_value.directory.Entries[coord]
		if !ok {
			return 404, headers, nil
		}
		tile_req := Request{kind: Tile, key: Key{name: name, rng: offsetlen}, value: make(chan Datum, 1)}
		loop.reqs <- tile_req
		tile_value := <-tile_req.value
		if !tile_value.hit {
			misses++
		}
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
	headers["Content-Type"] = content_type
	headers["Pmap-Cache-Misses"] = strconv.Itoa(misses)

	var body []byte
	if ext == "pbf" {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(tile)
		zw.Close()

		body = buf.Bytes()
		headers["Content-Length"] = strconv.Itoa(len(body))
		headers["Content-Encoding"] = "gzip"
	} else {
		body = tile
		headers["Content-Length"] = strconv.Itoa(len(tile))
	}
	elapsed := time.Since(start)
	loop.logger.Printf("served %s/%d/%d/%d in %s", name, z, x, y, elapsed)

	return 200, headers, body
}

func (fetcher FileFetcher) Do(key Key, readFunc func(io.Reader)) bool {
	f, err := os.Open(path.Join(fetcher.path, key.name+".pmtiles"))
	if err != nil {
		return false
	}
	_, err = f.Seek(int64(key.rng.Offset), 0)
	if err != nil {
		return false
	}
	defer f.Close()
	readFunc(f)
	return true
}

func main() {
	port := flag.String("p", "8080", "port to serve on")
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

	loop := NewLoop(path, logger, cacheSize, cors)
	loop.Start()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		status_code, headers, body := loop.Get(r.URL.Path)
		for k, v := range headers {
			w.Header().Set(k,v)
		}
		w.WriteHeader(status_code)
		w.Write(body)
	})

	logger.Printf("Serving %s on HTTP port: %s\n", path, *port)
	logger.Fatal(http.ListenAndServe(":"+*port, nil))
}
