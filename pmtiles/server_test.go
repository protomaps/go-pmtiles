package pmtiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"log"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func fakeArchive(t *testing.T, header HeaderV3, metadata map[string]interface{}, tiles map[Zxy][]byte, leaves bool) []byte {
	byTileID := make(map[uint64][]byte)
	keys := make([]uint64, 0, len(tiles))
	for zxy, bytes := range tiles {
		header.MaxZoom = max(header.MaxZoom, zxy.Z)
		id := ZxyToID(zxy.Z, zxy.X, zxy.Y)
		byTileID[id] = bytes
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	resolver := newResolver(false, false)
	tileDataBytes := make([]byte, 0)
	for _, id := range keys {
		tileBytes := byTileID[id]
		resolver.AddTileIsNew(id, tileBytes)
		tileDataBytes = append(tileDataBytes, tileBytes...)
	}

	var metadataBytes []byte
	{
		metadataBytesUncompressed, err := json.Marshal(metadata)
		assert.Nil(t, err)
		var b bytes.Buffer
		w, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
		w.Write(metadataBytesUncompressed)
		w.Close()
		metadataBytes = b.Bytes()
	}
	var rootBytes []byte
	var leavesBytes []byte
	if leaves {
		rootBytes, leavesBytes, _ = buildRootsLeaves(resolver.Entries, 1)
	} else {
		rootBytes = serializeEntries(resolver.Entries)
		leavesBytes = make([]byte, 0)
	}

	header.InternalCompression = Gzip
	if header.TileType == Mvt {
		header.TileCompression = Gzip
	}

	header.RootOffset = HeaderV3LenBytes
	header.RootLength = uint64(len(rootBytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.MetadataLength = uint64(len(metadataBytes))
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(leavesBytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength
	header.TileDataLength = resolver.Offset

	archiveBytes := serializeHeader(header)
	archiveBytes = append(archiveBytes, rootBytes...)
	archiveBytes = append(archiveBytes, metadataBytes...)
	archiveBytes = append(archiveBytes, leavesBytes...)
	archiveBytes = append(archiveBytes, tileDataBytes...)
	if len(archiveBytes) < 16384 {
		archiveBytes = append(archiveBytes, make([]byte, 16384-len(archiveBytes))...)
	}
	return archiveBytes
}

func newServer(t *testing.T) (mockBucket, *Server) {
	bucket := mockBucket{make(map[string][]byte)}
	server, err := NewServerWithBucket(bucket, "", log.Default(), 10, "", "tiles.example.com")
	assert.Nil(t, err)
	server.Start()
	return bucket, server
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
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{}, false)

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
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{
		"vector_layers": []map[string]string{{"id": "layer1"}},
		"attribution":   "Attribution",
		"description":   "Description",
		"name":          "Name",
		"version":       "1.0",
	}, map[Zxy][]byte{}, false)

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

func TestReadTiles(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3},
	}, false)

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
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
		{4, 1, 2}: {1, 2, 3},
	}, true)

	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1, 2, 3}, data)
	statusCode, _, data = server.Get(context.Background(), "/archive/4/1/2.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{1, 2, 3}, data)
	statusCode, _, _ = server.Get(context.Background(), "/archive/3/1/2.mvt")
	assert.Equal(t, 204, statusCode)
}

func TestInvalidateCacheOnTileRequest(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1, 2, 3},
	}, false)

	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1, 2, 3}, data)

	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {4, 5, 6, 7},
	}, false)

	statusCode, _, data = server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{4, 5, 6, 7}, data)
}

func TestInvalidateCacheOnDirRequest(t *testing.T) {
	mockBucket, server := newServer(t)
	header := HeaderV3{
		TileType: Mvt,
	}
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, true)

	// cache first leaf dir
	statusCode, _, data := server.Get(context.Background(), "/archive/0/0/0.mvt")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, []byte{0, 1}, data)

	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {4, 5},
		{1, 1, 1}: {6, 7},
	}, false)

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
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false)
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
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false)
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
	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{
		"meta": "data",
	}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false)
	statusCode, _, data := server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"meta": "data"
	}`, string(data))

	mockBucket.items["archive.pmtiles"] = fakeArchive(t, header, map[string]interface{}{
		"meta": "data2",
	}, map[Zxy][]byte{
		{0, 0, 0}: {0, 1},
		{1, 1, 1}: {2, 3},
	}, false)
	statusCode, _, data = server.Get(context.Background(), "/archive/metadata")
	assert.Equal(t, 200, statusCode)
	assert.JSONEq(t, `{
		"meta": "data2"
	}`, string(data))
}
