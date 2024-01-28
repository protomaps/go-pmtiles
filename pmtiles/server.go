package pmtiles

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
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
	ok        bool
}

type Response struct {
	key   CacheKey
	value CachedValue
	size  int
	ok    bool
}

type Server struct {
	reqs      chan Request
	bucket    Bucket
	logger    *log.Logger
	cacheSize int
	cors      string
	publicURL string
}

func NewServer(bucketURL string, prefix string, logger *log.Logger, cacheSize int, cors string, publicURL string) (*Server, error) {

	ctx := context.Background()

	bucketURL, _, err := NormalizeBucketKey(bucketURL, prefix, "")

	if err != nil {
		return nil, err
	}

	bucket, err := OpenBucket(ctx, bucketURL, prefix)

	if err != nil {
		return nil, err
	}

	return NewServerWithBucket(bucket, prefix, logger, cacheSize, cors, publicURL)
}

func NewServerWithBucket(bucket Bucket, prefix string, logger *log.Logger, cacheSize int, cors string, publicURL string) (*Server, error) {

	reqs := make(chan Request, 8)

	l := &Server{
		reqs:      reqs,
		bucket:    bucket,
		logger:    logger,
		cacheSize: cacheSize,
		cors:      cors,
		publicURL: publicURL,
	}

	return l, nil
}

func (server *Server) Start() {
	go func() {
		cache := make(map[CacheKey]*list.Element)
		inflight := make(map[CacheKey][]Request)
		resps := make(chan Response, 8)
		evictList := list.New()
		totalSize := 0
		ctx := context.Background()

		cacheSize := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pmtiles",
			Subsystem: "cache",
			Name:      "size",
			Help:      "Current number or directories in the cache",
		})
		prometheus.MustRegister(cacheSize)

		for {
			select {
			case req := <-server.reqs:
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
						isRoot := (key.offset == 0 && key.length == 0)

						offset := int64(key.offset)
						length := int64(key.length)

						if isRoot {
							offset = 0
							length = 16384
						}

						server.logger.Printf("fetching %s %d-%d", key.name, offset, length)
						r, err := server.bucket.NewRangeReader(ctx, key.name+".pmtiles", offset, length)

						// TODO: store away ETag
						if err != nil {
							ok = false
							resps <- Response{key: key, value: result}
							server.logger.Printf("failed to fetch %s %d-%d, %v", key.name, key.offset, key.length, err)
							return
						}
						defer r.Close()
						b, err := io.ReadAll(r)
						if err != nil {
							ok = false
							resps <- Response{key: key, value: result}
							server.logger.Printf("failed to fetch %s %d-%d, %v", key.name, key.offset, key.length, err)
							return
						}

						if isRoot {
							header, err := deserializeHeader(b[0:HeaderV3LenBytes])
							if err != nil {
								server.logger.Printf("parsing header failed: %v", err)
								return
							}

							// populate the root first before header
							rootEntries := deserializeEntries(bytes.NewBuffer(b[header.RootOffset : header.RootOffset+header.RootLength]))
							result2 := CachedValue{directory: rootEntries, ok: true}

							rootKey := CacheKey{name: key.name, offset: header.RootOffset, length: header.RootLength}
							resps <- Response{key: rootKey, value: result2, size: 24 * len(rootEntries), ok: true}

							result = CachedValue{header: header, ok: true}
							resps <- Response{key: key, value: result, size: 127, ok: true}
						} else {
							directory := deserializeEntries(bytes.NewBuffer(b))
							result = CachedValue{directory: directory, ok: true}
							resps <- Response{key: key, value: result, size: 24 * len(directory), ok: true}
						}

						server.logger.Printf("fetched %s %d-%d", key.name, key.offset, key.length)
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
						if totalSize < server.cacheSize*1000*1000 {
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
					cacheSize.Set(float64(len(cache)))
				}
			}
		}
	}()
}

func (server *Server) getHeaderMetadata(ctx context.Context, name string) (error, bool, HeaderV3, []byte) {
	rootReq := Request{key: CacheKey{name: name, offset: 0, length: 0}, value: make(chan CachedValue, 1)}
	server.reqs <- rootReq
	rootValue := <-rootReq.value
	header := rootValue.header

	if !rootValue.ok {
		return nil, false, HeaderV3{}, nil
	}

	r, err := server.bucket.NewRangeReader(ctx, name+".pmtiles", int64(header.MetadataOffset), int64(header.MetadataLength))
	if err != nil {
		return nil, false, HeaderV3{}, nil
	}
	defer r.Close()

	var metadataBytes []byte
	if header.InternalCompression == Gzip {
		metadataReader, _ := gzip.NewReader(r)
		defer metadataReader.Close()
		metadataBytes, err = io.ReadAll(metadataReader)
	} else if header.InternalCompression == NoCompression {
		metadataBytes, err = io.ReadAll(r)
	} else {
		return errors.New("Unknown compression"), true, HeaderV3{}, nil
	}

	return nil, true, header, metadataBytes
}

func (server *Server) getTileJSON(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	err, found, header, metadataBytes := server.getHeaderMetadata(ctx, name)

	if err != nil {
		return 500, httpHeaders, []byte("I/O Error")
	}

	if !found {
		return 404, httpHeaders, []byte("Archive not found")
	}

	var metadataMap map[string]interface{}
	json.Unmarshal(metadataBytes, &metadataMap)

	if server.publicURL == "" {
		return 501, httpHeaders, []byte("PUBLIC_URL must be set for TileJSON")
	}

	tilejsonBytes, err := CreateTilejson(header, metadataBytes, server.publicURL+"/"+name)
	if err != nil {
		return 500, httpHeaders, []byte("Error generating tilejson")
	}

	httpHeaders["Content-Type"] = "application/json"

	return 200, httpHeaders, tilejsonBytes
}

func (server *Server) getMetadata(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	err, found, _, metadataBytes := server.getHeaderMetadata(ctx, name)

	if err != nil {
		return 500, httpHeaders, []byte("I/O Error")
	}

	if !found {
		return 404, httpHeaders, []byte("Archive not found")
	}

	httpHeaders["Content-Type"] = "application/json"
	return 200, httpHeaders, metadataBytes
}

func (server *Server) getTile(ctx context.Context, httpHeaders map[string]string, name string, z uint8, x uint32, y uint32, ext string) (int, map[string]string, []byte) {
	rootReq := Request{key: CacheKey{name: name, offset: 0, length: 0}, value: make(chan CachedValue, 1)}
	server.reqs <- rootReq

	// https://golang.org/doc/faq#atomic_maps
	rootValue := <-rootReq.value
	header := rootValue.header

	if !rootValue.ok {
		return 404, httpHeaders, []byte("Archive not found")
	}

	if z < header.MinZoom || z > header.MaxZoom {
		return 404, httpHeaders, []byte("Tile not found")
	}

	switch header.TileType {
	case Mvt:
		if ext != "mvt" {
			return 400, httpHeaders, []byte("path mismatch: archive is type MVT (.mvt)")
		}
	case Png:
		if ext != "png" {
			return 400, httpHeaders, []byte("path mismatch: archive is type PNG (.png)")
		}
	case Jpeg:
		if ext != "jpg" {
			return 400, httpHeaders, []byte("path mismatch: archive is type JPEG (.jpg)")
		}
	case Webp:
		if ext != "webp" {
			return 400, httpHeaders, []byte("path mismatch: archive is type WebP (.webp)")
		}
	case Avif:
		if ext != "avif" {
			return 400, httpHeaders, []byte("path mismatch: archive is type AVIF (.avif)")
		}
	}

	tileID := ZxyToID(z, x, y)
	dirOffset, dirLen := header.RootOffset, header.RootLength

	for depth := 0; depth <= 3; depth++ {
		dirReq := Request{key: CacheKey{name: name, offset: dirOffset, length: dirLen}, value: make(chan CachedValue, 1)}
		server.reqs <- dirReq
		dirValue := <-dirReq.value
		directory := dirValue.directory
		entry, ok := findTile(directory, tileID)
		if !ok {
			break
		}

		if entry.RunLength > 0 {
			r, err := server.bucket.NewRangeReader(ctx, name+".pmtiles", int64(header.TileDataOffset+entry.Offset), int64(entry.Length))
			if err != nil {
				return 500, httpHeaders, []byte("Network error")
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				return 500, httpHeaders, []byte("I/O error")
			}
			if headerVal, ok := headerContentType(header); ok {
				httpHeaders["Content-Type"] = headerVal
			}
			if headerVal, ok := headerContentEncoding(header.TileCompression); ok {
				httpHeaders["Content-Encoding"] = headerVal
			}
			return 200, httpHeaders, b
		} else {
			dirOffset = header.LeafDirectoryOffset + entry.Offset
			dirLen = uint64(entry.Length)
		}
	}

	return 204, httpHeaders, nil
}

var tilePattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\/(\d+)\/(\d+)\/(\d+)\.([a-z]+)$`)
var metadataPattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\/metadata$`)
var tileJSONPattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\.json$`)

func parseTilePath(path string) (bool, string, uint8, uint32, uint32, string) {
	if res := tilePattern.FindStringSubmatch(path); res != nil {
		name := res[1]
		z, _ := strconv.ParseUint(res[2], 10, 8)
		x, _ := strconv.ParseUint(res[3], 10, 32)
		y, _ := strconv.ParseUint(res[4], 10, 32)
		ext := res[5]
		return true, name, uint8(z), uint32(x), uint32(y), ext
	}
	return false, "", 0, 0, 0, ""
}

func parseTilejsonPath(path string) (bool, string) {
	if res := tileJSONPattern.FindStringSubmatch(path); res != nil {
		name := res[1]
		return true, name
	}
	return false, ""
}

func parseMetadataPath(path string) (bool, string) {
	if res := metadataPattern.FindStringSubmatch(path); res != nil {
		name := res[1]
		return true, name
	}
	return false, ""
}

func (server *Server) Get(ctx context.Context, path string) (int, map[string]string, []byte) {
	httpHeaders := make(map[string]string)
	if len(server.cors) > 0 {
		httpHeaders["Access-Control-Allow-Origin"] = server.cors
	}

	if ok, key, z, x, y, ext := parseTilePath(path); ok {
		return server.getTile(ctx, httpHeaders, key, z, x, y, ext)
	}
	if ok, key := parseTilejsonPath(path); ok {
		return server.getTileJSON(ctx, httpHeaders, key)
	}
	if ok, key := parseMetadataPath(path); ok {
		return server.getMetadata(ctx, httpHeaders, key)
	}

	if path == "/" {
		return 204, httpHeaders, []byte{}
	}

	return 404, httpHeaders, []byte("Path not found")
}
