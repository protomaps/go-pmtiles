package pmtiles

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

var testResponse = []byte("bar")
var testHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write(testResponse)
})

func TestRegex(t *testing.T) {
	ok, key, z, x, y, ext := parseTilePath("/foo/0/0/0")
	assert.False(t, ok)
	ok, key, z, x, y, ext = parseTilePath("/foo/0/0/0.mvt")
	assert.True(t, ok)
	assert.Equal(t, key, "foo")
	assert.Equal(t, z, uint8(0))
	assert.Equal(t, x, uint32(0))
	assert.Equal(t, y, uint32(0))
	assert.Equal(t, ext, "mvt")
	ok, key, z, x, y, ext = parseTilePath("/foo/bar/0/0/0.mvt")
	assert.True(t, ok)
	assert.Equal(t, key, "foo/bar")
	assert.Equal(t, z, uint8(0))
	assert.Equal(t, x, uint32(0))
	assert.Equal(t, y, uint32(0))
	assert.Equal(t, ext, "mvt")
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
	ok, key, z, x, y, ext = parseTilePath("/!-_.*'()/0/0/0.mvt")
	assert.True(t, ok)
	assert.Equal(t, key, "!-_.*'()")
	assert.Equal(t, z, uint8(0))
	assert.Equal(t, x, uint32(0))
	assert.Equal(t, y, uint32(0))
	assert.Equal(t, ext, "mvt")
	ok, key = parseMetadataPath("/!-_.*'()/metadata")
	assert.True(t, ok)
	assert.Equal(t, key, "!-_.*'()")
	ok, key = parseTilejsonPath("/!-_.*'().json")
	assert.True(t, ok)
	assert.Equal(t, key, "!-_.*'()")
}

func newServer(t *testing.T) (mockBucket, *Server) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	bucket := mockBucket{make(map[string][]byte)}
	server, err := NewServerWithBucket(bucket, "", log.Default(), 10, "tiles.example.com")
	assert.Nil(t, err)
	server.Start()
	return bucket, server
}

func TestPostReturns405(t *testing.T) {
	_, server := newServer(t)
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/", nil)
	server.ServeHTTP(res, req)
	assert.Equal(t, 405, res.Code)
}

func TestMissingFileReturns404(t *testing.T) {
	_, server := newServer(t)
	statusCode, _, _ := server.Get(context.Background(), "/")
	assert.Equal(t, 204, statusCode)
	statusCode, _, _ = server.Get(context.Background(), "/archive.json")
	assert.Equal(t, 404, statusCode)
	statusCode, _, _ = server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 404, statusCode)
	statusCode, _, _ = server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 404, statusCode)
}

func TestMvtEmptyArchiveReads(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{}, false, Gzip)

	statusCode, _, _ := server.Get(context.Background(), "/")
	assert.Equal(t, 204, statusCode)
	statusCode, _, data := server.Get(context.Background(), "/archive.json")
	assert.JSONEq(t, `{
		"bounds": [0,0,0,0],
		"center": [0,0,0],
		"maxzoom": 0,
		"minzoom": 0,
		"scheme": "xyz",
		"tilejson": "3.0.0",
		"tiles": ["tiles.example.com/archive/{z}/{x}/{y}.mvt"],
		"vector_layers": null
	}`, string(data))
	assert.Equal(t, 200, statusCode)
	statusCode, _, data = server.Get(context.Background(), "/archive/metadata")
	assert.JSONEq(t, `{}`, string(data))
	assert.Equal(t, 200, statusCode)
	statusCode, _, _ = server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 204, statusCode)
}

func TestReadMetadata(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{
		"vector_layers": []map[string]string{{"id": "layer1"}},
		"attribution":   "Attribution",
		"description":   "Description",
		"name":          "Name",
		"version":       "1.0",
	}, map[Zxy][]byte{}, false, Gzip)

	statusCode, _, _ := server.Get(context.Background(), "/")
	assert.Equal(t, 204, statusCode)
	statusCode, _, data := server.Get(context.Background(), "/archive.json")
	assert.JSONEq(t, `{
		"attribution": "Attribution",
		"description": "Description",
		"name": "Name",
		"version": "1.0",
		"bounds": [0,0,0,0],
		"center": [0,0,0],
		"maxzoom": 0,
		"minzoom": 0,
		"scheme": "xyz",
		"tilejson": "3.0.0",
		"tiles": ["tiles.example.com/archive/{z}/{x}/{y}.mvt"],
		"vector_layers": [
			{"id": "layer1"}
		]
	}`, string(data))
	assert.Equal(t, 200, statusCode)
	statusCode, _, data = server.Get(context.Background(), "/archive/metadata")
	assert.JSONEq(t, `{
		"attribution": "Attribution",
		"description": "Description",
		"name": "Name",
		"version": "1.0",
		"vector_layers": [
			{"id": "layer1"}
		]
	}`, string(data))
}

func TestReadMetadataNoCompression(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{
		"vector_layers": []map[string]string{{"id": "layer1"}},
	}, map[Zxy][]byte{}, false, NoCompression)

	statusCode, _, data := server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"vector_layers": [
			{"id": "layer1"}
		]
	}`, string(data))
}

func TestReadTiles(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3},
	}, false, Gzip)

	statusCode, _, _ := server.Get(context.Background(), "/")
	assert.Equal(t, 204, statusCode)
	statusCode, _, _ = server.Get(context.Background(), "/archive.json")
	assert.Equal(t, 200, statusCode)
	statusCode, _, _ = server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 200, statusCode)
	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1, 2, 3}, data)
	statusCode, _, data = server.Get(context.Background(), "/archive/4/1/2.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{1, 2, 3}, data)
	statusCode, _, _ = server.Get(context.Background(), "/archive/3/1/2.mvt")
	assert.Equal(t, 204, statusCode)
}

func TestReadTilesFromLeaves(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3},
	}, true, Gzip)

	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1, 2, 3}, data)
	statusCode, _, data = server.Get(context.Background(), "/archive/4/1/2.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{1, 2, 3}, data)
	statusCode, _, _ = server.Get(context.Background(), "/archive/3/1/2.mvt")
	assert.Equal(t, 204, statusCode)
}

func TestReadTilesFromLeavesNoCompression(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3},
	}, true, NoCompression)

	statusCode, _, data := server.Get(context.Background(), "/archive/4/1/2.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{1, 2, 3}, data)
}

func TestInvalidateCacheOnTileRequest(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
	}, false, Gzip)

	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1, 2, 3}, data)

	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {4, 5, 6, 7},
	}, false, Gzip)

	statusCode, _, data = server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{4, 5, 6, 7}, data)
}

func TestInvalidateCacheOnDirRequest(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, true, Gzip)

	// cache first leaf dir
	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1}, data)

	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {4, 5},
		{1, 1, 1}: {6, 7},
	}, false, Gzip)

	// get etag mismatch on second leaf dir request
	statusCode, _, data = server.Get(context.Background(), "/archive/1/1/1.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{6, 7}, data)
	statusCode, _, data = server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{4, 5}, data)
}

func TestInvalidateCacheOnTileJSONRequest(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	statusCode, _, data := server.Get(context.Background(), "/archive.json")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"bounds": [0,0,0,0],
		"center": [0,0,0],
		"maxzoom": 1,
		"minzoom": 0,
		"scheme": "xyz",
		"tilejson": "3.0.0",
		"tiles": ["tiles.example.com/archive/{z}/{x}/{y}.mvt"],
		"vector_layers": null
	}`, string(data))

	header = HeaderV3{
		TileType:   Mvt,
		CenterZoom: 4,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	statusCode, _, data = server.Get(context.Background(), "/archive.json")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"bounds": [0,0,0,0],
		"center": [0,0,4],
		"maxzoom": 1,
		"minzoom": 0,
		"scheme": "xyz",
		"tilejson": "3.0.0",
		"tiles": ["tiles.example.com/archive/{z}/{x}/{y}.mvt"],
		"vector_layers": null
	}`, string(data))
}

func TestInvalidateCacheOnMetadataRequest(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{
		"meta": "data",
	}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	statusCode, _, data := server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"meta": "data"
	}`, string(data))

	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{
		"meta": "data2",
	}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false, Gzip)
	statusCode, _, data = server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"meta": "data2"
	}`, string(data))
}

func TestEtagResponsesFromTile(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3},
	}, false, Gzip)

	statusCode, headers000v1, _ := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	statusCode, headers412v1, _ := server.Get(context.Background(), "/archive/4/1/2.mvt")
	assert.Equal(t, 200, statusCode)
	statusCode, headers311v1, _ := server.Get(context.Background(), "/archive/3/1/1.mvt")
	assert.Equal(t, 204, statusCode)

	mockBucket.items["archive.pmtiles"] = fakeArchive(header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3, 4}, // different
	}, false, Gzip)

	statusCode, headers000v2, _ := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	statusCode, headers412v2, _ := server.Get(context.Background(), "/archive/4/1/2.mvt")
	assert.Equal(t, 200, statusCode)
	statusCode, headers311v2, _ := server.Get(context.Background(), "/archive/3/1/1.mvt")
	assert.Equal(t, 204, statusCode)

	// 204's have no etag
	assert.Equal(t, "", headers311v1["ETag"])
	assert.Equal(t, "", headers311v2["ETag"])

	// 000 and 311 didn't change
	assert.Equal(t, headers000v1["ETag"], headers000v2["ETag"])

	// 412 did change
	assert.NotEqual(t, headers412v1["ETag"], headers412v2["ETag"])

	// all are different
	assert.NotEqual(t, headers000v1["ETag"], headers311v1["ETag"])
	assert.NotEqual(t, headers000v1["ETag"], headers412v1["ETag"])
}

func TestSingleCorsOrigin(t *testing.T) {
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)
	req.Header.Add("Origin", "http://example.com")
	c := NewCors("http://example.com")
	c.Handler(testHandler).ServeHTTP(res, req)
	assert.Equal(t, 200, res.Code)
	assert.Equal(t, "http://example.com", res.Header().Get("Access-Control-Allow-Origin"))
}

func TestMultiCorsOrigin(t *testing.T) {
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "http://example2.com/foo", nil)
	req.Header.Add("Origin", "http://example2.com")
	c := NewCors("http://example.com,http://example2.com")
	c.Handler(testHandler).ServeHTTP(res, req)
	assert.Equal(t, 200, res.Code)
	assert.Equal(t, "http://example2.com", res.Header().Get("Access-Control-Allow-Origin"))
}

func TestWildcardCors(t *testing.T) {
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)
	req.Header.Add("Origin", "http://example.com")
	c := NewCors("*")
	c.Handler(testHandler).ServeHTTP(res, req)
	assert.Equal(t, 200, res.Code)
	assert.Equal(t, "*", res.Header().Get("Access-Control-Allow-Origin"))
}

func TestCorsOptions(t *testing.T) {
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("OPTIONS", "http://example.com/foo", nil)
	req.Header.Add("Origin", "http://example.com")
	req.Header.Add("Access-Control-Request-Method", "GET")
	c := NewCors("*")
	c.Handler(testHandler).ServeHTTP(res, req)
	assert.Equal(t, 204, res.Code)
	assert.Equal(t, "*", res.Header().Get("Access-Control-Allow-Origin"))
}
