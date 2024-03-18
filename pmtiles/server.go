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
	"net/http"
	"regexp"
	"strconv"
	"time"
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
	metrics   *metrics
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
		metrics:   createMetrics("", logger), // change scope string if there are multiple servers running in one process
	}

	return l, nil
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
		server.metrics.initCacheStats(server.cacheSize * 1000 * 1000)

		for {
			select {
			case req := <-server.reqs:
				if len(req.purgeEtag) > 0 {
					if _, dup := inflight[req.key]; !dup {
						server.metrics.reloadFile(req.key.name)
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
					server.metrics.updateCacheStats(totalSize, len(cache))
				}
				key := req.key
				isRoot := (key.offset == 0 && key.length == 0)
				kind := "leaf"
				if isRoot {
					kind = "root"
				}
				if val, ok := cache[key]; ok {
					evictList.MoveToFront(val)
					req.value <- val.Value.(*response).value
					server.metrics.cacheRequest(key.name, kind, "hit")
				} else if _, ok := inflight[key]; ok {
					inflight[key] = append(inflight[key], req)
					server.metrics.cacheRequest(key.name, kind, "hit") // treat inflight as a hit since it doesn't make a new server request
				} else {
					inflight[key] = []request{req}
					server.metrics.cacheRequest(key.name, kind, "miss")
					go func() {
						var result cachedValue

						offset := int64(key.offset)
						length := int64(key.length)

						if isRoot {
							offset = 0
							length = 16384
						}

						status := ""
						tracker := server.metrics.startBucketRequest(key.name, kind)
						defer func() { tracker.finish(ctx, status) }()

						server.logger.Printf("fetching %s %d-%d", key.name, offset, length)
						r, etag, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, key.name+".pmtiles", offset, length, key.etag)
						status = strconv.Itoa(statusCode)

						if err != nil {
							ok = false
							result.badEtag = isRefreshRequiredError(err)
							resps <- response{key: key, value: result}
							server.logger.Printf("failed to fetch %s %d-%d, %v", key.name, key.offset, key.length, err)
							return
						}
						defer r.Close()
						b, err := io.ReadAll(r)
						if err != nil {
							ok = false
							status = "error"
							resps <- response{key: key, value: result}
							server.logger.Printf("failed to fetch %s %d-%d, %v", key.name, key.offset, key.length, err)
							return
						}

						if isRoot {
							header, err := deserializeHeader(b[0:HeaderV3LenBytes])
							if err != nil {
								status = "error"
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
					server.metrics.updateCacheStats(totalSize, len(cache))
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

	status := ""
	tracker := server.metrics.startBucketRequest(name, "metadata")
	defer func() { tracker.finish(ctx, status) }()
	r, _, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.MetadataOffset), int64(header.MetadataLength), rootValue.etag)
	status = strconv.Itoa(statusCode)
	if isRefreshRequiredError(err) {
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
		status = "error"
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
	httpHeaders["ETag"] = generateEtag(tilejsonBytes)

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
	httpHeaders["ETag"] = generateEtag(metadataBytes)
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
			status := ""
			tracker := server.metrics.startBucketRequest(name, "tile")
			defer func() { tracker.finish(ctx, status) }()
			r, _, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.TileDataOffset+entry.Offset), int64(entry.Length), rootValue.etag)
			status = strconv.Itoa(statusCode)
			if isRefreshRequiredError(err) {
				return 500, httpHeaders, []byte("I/O Error"), rootValue.etag
			}
			// possible we have the header/directory cached but the archive has disappeared
			if err != nil {
				if isCanceled(ctx) {
					return 499, httpHeaders, []byte("Canceled"), ""
				}
				server.logger.Printf("failed to fetch tile %s %d-%d %v", name, entry.Offset, entry.Length, err)
				return 404, httpHeaders, []byte("Tile not found"), ""
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				status = "error"
				if isCanceled(ctx) {
					return 499, httpHeaders, []byte("Canceled"), ""
				}
				return 500, httpHeaders, []byte("I/O error"), ""
			}

			httpHeaders["ETag"] = generateEtag(b)
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

func isRefreshRequiredError(err error) bool {
	_, ok := err.(*RefreshRequiredError)
	return ok
}

func isCanceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
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

func (server *Server) get(ctx context.Context, unsanitizedPath string) (archive, handler string, status int, headers map[string]string, data []byte) {
	handler = ""
	archive = ""
	headers = make(map[string]string)
	if len(server.cors) > 0 {
		headers["Access-Control-Allow-Origin"] = server.cors
	}

	if ok, key, z, x, y, ext := parseTilePath(unsanitizedPath); ok {
		archive, handler = key, "tile"
		status, headers, data = server.getTile(ctx, headers, key, z, x, y, ext)
	} else if ok, key := parseTilejsonPath(unsanitizedPath); ok {
		archive, handler = key, "tilejson"
		status, headers, data = server.getTileJSON(ctx, headers, key)
	} else if ok, key := parseMetadataPath(unsanitizedPath); ok {
		archive, handler = key, "metadata"
		status, headers, data = server.getMetadata(ctx, headers, key)
	} else if unsanitizedPath == "/" {
		handler, status, data = "/", 204, []byte{}
	} else {
		handler, status, data = "404", 404, []byte("Path not found")
	}

	return
}

// Get a response for the given path.
// Return status code, HTTP headers, and body.
func (server *Server) Get(ctx context.Context, path string) (int, map[string]string, []byte) {
	tracker := server.metrics.startRequest()
	archive, handler, status, headers, data := server.get(ctx, path)
	tracker.finish(ctx, archive, handler, status, len(data), true)
	return status, headers, data
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Serve an HTTP response from the archive
func (server *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) int {
	tracker := server.metrics.startRequest()
	if r.Method == http.MethodOptions {
		if len(server.cors) > 0 {
			w.Header().Set("Access-Control-Allow-Origin", server.cors)
		}
		w.WriteHeader(204)
		tracker.finish(r.Context(), "", r.Method, 204, 0, false)
		return 204
	} else if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(405)
		tracker.finish(r.Context(), "", r.Method, 405, 0, false)
		return 405
	}
	archive, handler, statusCode, headers, body := server.get(r.Context(), r.URL.Path)
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	if statusCode == 200 {
		lrw := &loggingResponseWriter{w, 200}
		// handle if-match, if-none-match request headers based on response etag
		http.ServeContent(
			lrw, r,
			"",                // name used to infer content-type, but we've already set that
			time.UnixMilli(0), // ignore setting last-modified time and handling if-modified-since headers
			bytes.NewReader(body),
		)
		statusCode = lrw.statusCode
	} else {
		w.WriteHeader(statusCode)
		w.Write(body)
	}
	tracker.finish(r.Context(), archive, handler, statusCode, len(body), true)

	return statusCode
}
