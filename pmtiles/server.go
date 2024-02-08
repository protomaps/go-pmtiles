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

type cacheKey struct {
	name   string
	etag   string
	offset uint64 // is 0 for header
	length uint64 // is 0 for header
}

type request struct {
	key       cacheKey
	value     chan cachedValue
	purgeEtag string
}

type cachedValue struct {
	header    HeaderV3
	directory []EntryV3
	etag      string
	ok        bool
	badEtag   bool
}

type response struct {
	key   cacheKey
	value cachedValue
	size  int
	ok    bool
}

// Server is an HTTP server for tiles and metadata.
type Server struct {
	reqs      chan request
	bucket    Bucket
	logger    *log.Logger
	cacheSize int
	cors      string
	publicURL string
}

// NewServer creates a new pmtiles HTTP server.
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

// NewServerWithBucket creates a new HTTP server for a gocloud Bucket.
func NewServerWithBucket(bucket Bucket, _ string, logger *log.Logger, cacheSize int, cors string, publicURL string) (*Server, error) {

	reqs := make(chan request, 8)

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

func register[K prometheus.Collector](server *Server, metric K) K {
	if err := prometheus.Register(metric); err != nil {
		server.logger.Println(err)
	}
	return metric
}

// Start the server HTTP listener.
func (server *Server) Start() {

	go func() {
		cache := make(map[cacheKey]*list.Element)
		inflight := make(map[cacheKey][]request)
		resps := make(chan response, 8)
		evictList := list.New()
		totalSize := 0
		ctx := context.Background()

		cacheSize := register(server, prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pmtiles",
			Subsystem: "cache",
			Name:      "size",
			Help:      "Current number or directories in the cache",
		}))

		for {
			select {
			case req := <-server.reqs:
				if len(req.purgeEtag) > 0 {
					if _, dup := inflight[req.key]; !dup {
						server.logger.Printf("re-fetching directories for changed file %s", req.key.name)
					}
					for k, v := range cache {
						resp := v.Value.(*response)
						if k.name == req.key.name && (k.etag == req.purgeEtag || resp.value.etag == req.purgeEtag) {
							evictList.Remove(v)
							delete(cache, k)
							totalSize -= resp.size
						}
					}
					cacheSize.Set(float64(len(cache)))
				}
				key := req.key
				if val, ok := cache[key]; ok {
					evictList.MoveToFront(val)
					req.value <- val.Value.(*response).value
				} else if _, ok := inflight[key]; ok {
					inflight[key] = append(inflight[key], req)
				} else {
					inflight[key] = []request{req}
					go func() {
						var result cachedValue
						isRoot := (key.offset == 0 && key.length == 0)

						offset := int64(key.offset)
						length := int64(key.length)

						if isRoot {
							offset = 0
							length = 16384
						}

						server.logger.Printf("fetching %s %d-%d", key.name, offset, length)
						r, etag, err := server.bucket.NewRangeReaderEtag(ctx, key.name+".pmtiles", offset, length, key.etag)

						if err != nil {
							ok = false
							result.badEtag = isRefreshRequredError(err)
							resps <- response{key: key, value: result}
							server.logger.Printf("failed to fetch %s %d-%d, %v", key.name, key.offset, key.length, err)
							return
						}
						defer r.Close()
						b, err := io.ReadAll(r)
						if err != nil {
							ok = false
							resps <- response{key: key, value: result}
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
							result2 := cachedValue{directory: rootEntries, ok: true, etag: etag}

							rootKey := cacheKey{name: key.name, offset: header.RootOffset, length: header.RootLength}
							resps <- response{key: rootKey, value: result2, size: 24 * len(rootEntries), ok: true}

							result = cachedValue{header: header, ok: true, etag: etag}
							resps <- response{key: key, value: result, size: 127, ok: true}
						} else {
							directory := deserializeEntries(bytes.NewBuffer(b))
							result = cachedValue{directory: directory, ok: true, etag: etag}
							resps <- response{key: key, value: result, size: 24 * len(directory), ok: true}
						}

						server.logger.Printf("fetched %s %d-%d", key.name, key.offset, length)
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
							kv := ent.Value.(*response)
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

func (server *Server) getHeaderMetadata(ctx context.Context, name string) (bool, HeaderV3, []byte, error) {
	found, header, metadataBytes, purgeEtag, err := server.getHeaderMetadataAttempt(ctx, name, "")
	if len(purgeEtag) > 0 {
		found, header, metadataBytes, _, err = server.getHeaderMetadataAttempt(ctx, name, purgeEtag)
	}
	return found, header, metadataBytes, err
}

func (server *Server) getHeaderMetadataAttempt(ctx context.Context, name, purgeEtag string) (bool, HeaderV3, []byte, string, error) {
	rootReq := request{key: cacheKey{name: name, offset: 0, length: 0}, value: make(chan cachedValue, 1), purgeEtag: purgeEtag}
	server.reqs <- rootReq
	rootValue := <-rootReq.value
	header := rootValue.header

	if !rootValue.ok {
		return false, HeaderV3{}, nil, "", nil
	}

	r, _, err := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.MetadataOffset), int64(header.MetadataLength), rootValue.etag)
	if isRefreshRequredError(err) {
		return false, HeaderV3{}, nil, rootValue.etag, nil
	}
	if err != nil {
		return false, HeaderV3{}, nil, "", nil
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
		return true, HeaderV3{}, nil, "", errors.New("unknown compression")
	}

	return true, header, metadataBytes, "", nil
}

func (server *Server) getTileJSON(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	found, header, metadataBytes, err := server.getHeaderMetadata(ctx, name)

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

	tilejsonBytes, err := CreateTileJSON(header, metadataBytes, server.publicURL+"/"+name)
	if err != nil {
		return 500, httpHeaders, []byte("Error generating tilejson")
	}

	httpHeaders["Content-Type"] = "application/json"

	return 200, httpHeaders, tilejsonBytes
}

func (server *Server) getMetadata(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	found, _, metadataBytes, err := server.getHeaderMetadata(ctx, name)

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
	status, headers, data, purgeEtag := server.getTileAttempt(ctx, httpHeaders, name, z, x, y, ext, "")
	if len(purgeEtag) > 0 {
		// file has new etag, retry once force-purging the etag that is no longer value
		status, headers, data, _ = server.getTileAttempt(ctx, httpHeaders, name, z, x, y, ext, purgeEtag)
	}
	return status, headers, data
}
func (server *Server) getTileAttempt(ctx context.Context, httpHeaders map[string]string, name string, z uint8, x uint32, y uint32, ext string, purgeEtag string) (int, map[string]string, []byte, string) {
	rootReq := request{key: cacheKey{name: name, offset: 0, length: 0}, value: make(chan cachedValue, 1), purgeEtag: purgeEtag}
	server.reqs <- rootReq

	// https://golang.org/doc/faq#atomic_maps
	rootValue := <-rootReq.value
	header := rootValue.header

	if !rootValue.ok {
		return 404, httpHeaders, []byte("Archive not found"), ""
	}

	if z < header.MinZoom || z > header.MaxZoom {
		return 404, httpHeaders, []byte("Tile not found"), ""
	}

	switch header.TileType {
	case Mvt:
		if ext != "mvt" {
			return 400, httpHeaders, []byte("path mismatch: archive is type MVT (.mvt)"), ""
		}
	case Png:
		if ext != "png" {
			return 400, httpHeaders, []byte("path mismatch: archive is type PNG (.png)"), ""
		}
	case Jpeg:
		if ext != "jpg" {
			return 400, httpHeaders, []byte("path mismatch: archive is type JPEG (.jpg)"), ""
		}
	case Webp:
		if ext != "webp" {
			return 400, httpHeaders, []byte("path mismatch: archive is type WebP (.webp)"), ""
		}
	case Avif:
		if ext != "avif" {
			return 400, httpHeaders, []byte("path mismatch: archive is type AVIF (.avif)"), ""
		}
	}

	tileID := ZxyToID(z, x, y)
	dirOffset, dirLen := header.RootOffset, header.RootLength

	for depth := 0; depth <= 3; depth++ {
		dirReq := request{key: cacheKey{name: name, offset: dirOffset, length: dirLen, etag: rootValue.etag}, value: make(chan cachedValue, 1)}
		server.reqs <- dirReq
		dirValue := <-dirReq.value
		if dirValue.badEtag {
			return 500, httpHeaders, []byte("I/O Error"), rootValue.etag
		}
		directory := dirValue.directory
		entry, ok := findTile(directory, tileID)
		if !ok {
			break
		}

		if entry.RunLength > 0 {
			r, _, err := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.TileDataOffset+entry.Offset), int64(entry.Length), rootValue.etag)
			if isRefreshRequredError(err) {
				return 500, httpHeaders, []byte("I/O Error"), rootValue.etag
			}
			// possible we have the header/directory cached but the archive has disappeared
			if err != nil {
				server.logger.Printf("failed to fetch tile %s %d-%d", name, entry.Offset, entry.Length)
				return 404, httpHeaders, []byte("archive not found"), ""
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				return 500, httpHeaders, []byte("I/O error"), ""
			}
			if headerVal, ok := headerContentType(header); ok {
				httpHeaders["Content-Type"] = headerVal
			}
			if headerVal, ok := headerContentEncoding(header.TileCompression); ok {
				httpHeaders["Content-Encoding"] = headerVal
			}
			return 200, httpHeaders, b, ""
		}
		dirOffset = header.LeafDirectoryOffset + entry.Offset
		dirLen = uint64(entry.Length)
	}
	return 204, httpHeaders, nil, ""
}

func isRefreshRequredError(err error) bool {
	_, ok := err.(*RefreshRequiredError)
	return ok
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

// Get a response for the given path.
// Return status code, HTTP headers, and body.
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
