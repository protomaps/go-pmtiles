package pmtiles

import (
	"bytes"
	"container/list"
	"context"
	"gocloud.dev/blob"
	"io"
	"log"
	"regexp"
	"strconv"
)

type CacheKey struct {
	name   string
	offset uint64 // is 0 for header
	length uint64 // is 0 for header
}

type Request struct {
	key   CacheKey
	value chan CachedValue
}

type CachedValue struct {
	header    HeaderV3
	directory []EntryV3
	etag      string
}

type Response struct {
	key   CacheKey
	value CachedValue
	size  int
	ok    bool
}

// type HTTPFetcher struct {
// 	bucket string
// 	client *http.Client
// }

// func (fetcher HTTPFetcher) Do(key Key, readFunc func(io.Reader)) bool {
// 	archive := fetcher.bucket + "/" + key.name + ".pmtiles"
// 	fetch, _ := http.NewRequest("GET", archive, nil)
// 	end := key.rng.Offset + uint64(key.rng.Length) - 1
// 	range_header := fmt.Sprintf("bytes=%d-%d", key.rng.Offset, end)
// 	fetch.Header.Add("Range", range_header)
// 	fetch_resp, err := fetcher.client.Do(fetch)
// 	if err != nil || fetch_resp.StatusCode >= 300 || fetch_resp.ContentLength != int64(key.rng.Length) {
// 		return false
// 	}
// 	defer fetch_resp.Body.Close()
// 	readFunc(fetch_resp.Body)
// 	return true
// }

type Loop struct {
	reqs      chan Request
	bucket    *blob.Bucket
	logger    *log.Logger
	cacheSize int
	cors      string
}

func NewLoop(path string, logger *log.Logger, cacheSize int, cors string) Loop {
	reqs := make(chan Request, 8)

	ctx := context.Background()

	// TODO: handle single-file mode as well as public HTTP endpoints
	bucket, err := blob.OpenBucket(ctx, "file://.")
	if err != nil {
		logger.Fatal(err)
	}

	return Loop{reqs: reqs, bucket: bucket, logger: logger, cacheSize: cacheSize, cors: cors}
}

func (loop Loop) Start() {
	go func() {
		cache := make(map[CacheKey]*list.Element)
		inflight := make(map[CacheKey][]Request)
		resps := make(chan Response, 8)
		evictList := list.New()
		totalSize := 0
		ctx := context.Background()

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
						var result CachedValue
						is_root := (key.offset == 0 && key.length == 0)

						offset := int64(key.offset)
						length := int64(key.length)

						if is_root {
							offset = 0
							length = 16384
						}

						r, err := loop.bucket.NewRangeReader(ctx, key.name+".pmtiles", offset, length, nil)

						// TODO: store away ETag
						if err != nil {
							ok = false
							resps <- Response{key: key, value: result, size: 0, ok: true}
							loop.logger.Printf("failed to fetch %s %d-%d", key.name, key.offset, key.length)
							return
						}
						b, err := io.ReadAll(r)
						if err != nil {
							ok = false
							resps <- Response{key: key, value: result, size: 0, ok: true}
							loop.logger.Printf("failed to fetch %s %d-%d", key.name, key.offset, key.length)
							return
						}

						if is_root {
							header := deserialize_header(b[0:HEADERV3_LEN_BYTES])
							result = CachedValue{header: header}
							resps <- Response{key: key, value: result, size: 127, ok: true}

							root_entries := deserialize_entries(bytes.NewBuffer(b[header.RootOffset : header.RootOffset+header.RootLength]))
							result2 := CachedValue{directory: root_entries}

							root_key := CacheKey{name: key.name, offset: header.RootOffset, length: header.RootLength}
							resps <- Response{key: root_key, value: result2, size: 24 * len(root_entries), ok: true}
						} else {
							directory := deserialize_entries(bytes.NewBuffer(b))
							result = CachedValue{directory: directory}
							resps <- Response{key: key, value: result, size: 24 * len(directory), ok: true}
						}

						loop.logger.Printf("fetched %s %d-%d", key.name, key.offset, key.length)
					}()
				}
			case resp := <-resps:
				key := resp.key
				// check if there are any requests waiting on the key
				for _, v := range inflight[key] {
					v.value <- resp.value
				}
				delete(inflight, key)

				if resp.ok {
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

func setHTTPHeaders(header HeaderV3, headers map[string]string) {
	switch header.TileType {

	}
	// var content_type string
	// switch ext {
	// case "jpg":
	// 	content_type = "image/jpeg"
	// case "png":
	// 	content_type = "image/png"
	// case "pbf":
	// 	content_type = "application/x-protobuf"
	// }
	// headers["Content-Type"] = content_type
	// 	headers["Content-Length"] = strconv.Itoa(len(body))
	// 	headers["Content-Encoding"] = "gzip"
	// } else {
	// 	headers["Content-Length"] = strconv.Itoa(len(tile))
	// }
}

func (loop Loop) Get(ctx context.Context, path string) (int, map[string]string, []byte) {
	http_headers := make(map[string]string)
	// if len(loop.cors) > 0 {
	// 	headers["Access-Control-Allow-Origin"] = loop.cors
	// }
	rPath := regexp.MustCompile(`\/(?P<NAME>[-A-Za-z0-9_]+)\/(?P<Z>\d+)\/(?P<X>\d+)\/(?P<Y>\d+)\.(?P<EXT>png|pbf|jpg)`)
	res := rPath.FindStringSubmatch(path)

	// if len(res) == 0 {
	// 	mPath := regexp.MustCompile(`\/(?P<NAME>[-A-Za-z0-9_]+)\/metadata`)
	// 	res = mPath.FindStringSubmatch(path)

	if len(res) == 0 {
		return 404, http_headers, nil
	}

	// 	name := res[1]
	// 	root_req := Request{kind: Root, key: Key{name: name, rng: Range{Offset: 0, Length: 512000}}, value: make(chan Datum, 1)}
	// 	loop.reqs <- root_req
	// 	root_value := <-root_req.value
	// 	if !root_value.hit {
	// 		misses++
	// 	}
	// 	headers["Content-Length"] = strconv.Itoa(len(root_value.bytes))
	// 	headers["Content-Type"] = "application/json"
	// 	headers["Pmap-Cache-Misses"] = strconv.Itoa(misses)
	// 	return 200, headers, root_value.bytes
	// }

	name := res[1]

	root_req := Request{key: CacheKey{name: name, offset: 0, length: 0}, value: make(chan CachedValue, 1)}
	loop.reqs <- root_req

	// https://golang.org/doc/faq#atomic_maps
	root_value := <-root_req.value
	header := root_value.header

	z, _ := strconv.ParseUint(res[2], 10, 8)
	x, _ := strconv.ParseUint(res[3], 10, 32)
	y, _ := strconv.ParseUint(res[4], 10, 32)

	tile_id := ZxyToId(uint8(z), uint32(x), uint32(y))
	dir_offset, dir_len := header.RootOffset, header.RootLength

	for depth := 0; depth <= 2; depth++ {
		dir_req := Request{key: CacheKey{name: name, offset: dir_offset, length: dir_len}, value: make(chan CachedValue, 1)}
		loop.reqs <- dir_req
		dir_value := <-dir_req.value
		directory := dir_value.directory
		entry, ok := find_tile(directory, tile_id)
		if ok {
			if entry.RunLength > 0 {
				r, err := loop.bucket.NewRangeReader(ctx, name+".pmtiles", int64(header.TileDataOffset+entry.Offset), int64(entry.Length), nil)
				defer r.Close()
				if err != nil {
					return 500, http_headers, nil
				}
				b, err := io.ReadAll(r)
				if err != nil {
					return 500, http_headers, nil
				}
				setHTTPHeaders(header, http_headers)
				return 200, http_headers, b
			} else {
				dir_offset = header.LeafDirectoryOffset + entry.Offset
				dir_len = uint64(entry.Length)
			}
		} else {
			break
		}
	}

	return 404, http_headers, nil
}
