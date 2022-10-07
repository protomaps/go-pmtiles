package pmtiles

import (
	"testing"
)

func TestResolver(t *testing.T) {
	resolver := NewResolver()
	resolver.AddTileIsNew(1, []byte{0x1, 0x2})
	if len(resolver.Entries) != 1 {
		t.Fatalf("expected length 1")
	}
	resolver.AddTileIsNew(2, []byte{0x1, 0x3})
	if resolver.Offset != 52 {
		t.Fatalf("expected ending offset (total size) to be 52, was %d", resolver.Offset)
	}
	is_new, _ := resolver.AddTileIsNew(3, []byte{0x1, 0x2})
	if is_new {
		t.Fatalf("expected deduplication")
	}
	if resolver.Offset != 52 {
		t.Fatalf("expected ending offset (total size) to be 4, was %d", resolver.Offset)
	}
	resolver.AddTileIsNew(4, []byte{0x1, 0x2})
	if len(resolver.Entries) != 3 {
		t.Fatalf("expected length with RLE to be 3")
	}
	resolver.AddTileIsNew(6, []byte{0x1, 0x2})
	if len(resolver.Entries) != 4 {
		t.Fatalf("expected length with RLE to be 4 with a skip")
	}
}

func TestV2UpgradeBarebones(t *testing.T) {
	header, json_metadata, err := v2_to_header_json(map[string]interface{}{
		"minzoom":     4,
		"maxzoom":     5,
		"bounds":      "-180.0,-85,178,83",
		"attribution": "abcd",
	}, []byte{0x1f, 0x8b, 0x0, 0x0})
	if err != nil {
		t.Fatalf("parsing error %s", err)
	}
	if header.MinZoom != 4 {
		t.Fatalf("expected minzoom=4, was %d", header.MinZoom)
	}
	if header.MaxZoom != 5 {
		t.Fatalf("expected maxzoom=5, was %d", header.MaxZoom)
	}
	if _, ok := json_metadata["attribution"]; !ok {
		t.Fatalf("expected key in result")
	}
	if _, ok := json_metadata["minzoom"]; ok {
		t.Fatalf("expected minzoom not in result")
	}
	if _, ok := json_metadata["maxzoom"]; ok {
		t.Fatalf("expected maxzoom not in result")
	}
	if header.MinLonE7 != -180*10000000 {
		t.Fatalf(`expected min lon`)
	}
	if header.MinLatE7 != -85*10000000 {
		t.Fatalf(`expected min lat`)
	}
	if header.MaxLonE7 != 178*10000000 {
		t.Fatalf(`expected max lon`)
	}
	if header.MaxLatE7 != 83*10000000 {
		t.Fatalf(`expected max lat`)
	}
	if _, ok := json_metadata["bounds"]; ok {
		t.Fatalf("expected bounds not in result")
	}
	if header.CenterLatE7 != -1*10000000 {
		t.Fatalf(`expected default center lat -1`)
	}
	if header.CenterLonE7 != -1*10000000 {
		t.Fatalf(`expected default center lat -1`)
	}
	if header.CenterZoom != 4 {
		t.Fatalf(`expected default center zoom = minzoom`)
	}
	if header.TileCompression != Gzip {
		t.Fatalf(`expected infer gzip`)
	}
	if header.TileType != Mvt {
		t.Fatalf(`expected infer mvt`)
	}
}

func TestV2UpgradeStrings(t *testing.T) {
	header, _, err := v2_to_header_json(map[string]interface{}{
		"minzoom":     "0",
		"maxzoom":     "14",
		"bounds":      "-180.0,-85,178,83",
	}, []byte{0x1f, 0x8b, 0x0, 0x0})
	if err != nil {
		t.Fatalf("parsing error %s", err)
	}
	if header.MinZoom != 0 {
		t.Fatalf("expected minzoom=0, was %d", header.MinZoom)
	}
	if header.MaxZoom != 14 {
		t.Fatalf("expected maxzoom=14, was %d", header.MaxZoom)
	}
}

func TestV2UpgradeExtra(t *testing.T) {
	// with the fields tippecanoe usually has
	header, json_metadata, err := v2_to_header_json(map[string]interface{}{
		"minzoom":     1,
		"maxzoom":     2,
		"bounds":      "-180.0,-85,180,85",
		"center":      "-122.1906,37.7599,11",
		"format":      "pbf",
		"compression": "gzip",
	}, []byte{0x0, 0x0, 0x0, 0x0})
	if err != nil {
		t.Fatalf("parsing error %s", err)
	}
	if header.CenterLonE7 != -122.1906*10000000 {
		t.Fatalf(`expected center lon`)
	}
	if header.CenterLatE7 != 37.7599*10000000 {
		t.Fatalf(`expected center lat`)
	}
	if header.CenterZoom != 11 {
		t.Fatalf(`expected center zoom`)
	}
	if _, ok := json_metadata["center"]; ok {
		t.Fatalf("expected center not in result")
	}
}

func TestV2UpgradeInfer(t *testing.T) {
	header, _, err := v2_to_header_json(map[string]interface{}{
		"minzoom": 1,
		"maxzoom": 2,
		"bounds":  "-180.0,-85,180,85",
	}, []byte{0xff, 0xd8, 0xff, 0xe0})
	if err != nil || header.TileType != Jpeg || header.TileCompression != NoCompression {
		t.Fatalf("expected inferred tile type")
	}

	header, _, err = v2_to_header_json(map[string]interface{}{
		"minzoom": 1,
		"maxzoom": 2,
		"bounds":  "-180.0,-85,180,85",
	}, []byte{0x89, 0x50, 0x4e, 0x47})
	if err != nil || header.TileType != Png || header.TileCompression != NoCompression {
		t.Fatalf("expected inferred tile type")
	}
	header, _, err = v2_to_header_json(map[string]interface{}{
		"minzoom": 1,
		"maxzoom": 2,
		"bounds":  "-180.0,-85,180,85",
	}, []byte{0x00, 00, 00, 00})
	if header.TileType != Mvt || header.TileCompression != NoCompression {
		t.Fatalf("expected inferred tile type")
	}
	header, _, err = v2_to_header_json(map[string]interface{}{
		"minzoom": 1,
		"maxzoom": 2,
		"bounds":  "-180.0,-85,180,85",
	}, []byte{0x1f, 0x8b, 00, 00})
	if err != nil || header.TileType != Mvt || header.TileCompression != Gzip {
		t.Fatalf("expected inferred tile type")
	}
}

func TestMbtiles(t *testing.T) {
	header, json_metadata, err := mbtiles_to_header_json([]string{
		"name", "test_name",
		"format", "pbf",
		"bounds", "-180.0,-85,180,85",
		"center", "-122.1906,37.7599,11",
		"minzoom", "1",
		"maxzoom", "2",
		"attribution", "<div>abc</div>",
		"description", "a description",
		"type", "overlay",
		"version", "1",
		"json", "{\"vector_layers\":[{\"abc\":123}],\"tilestats\":{\"def\":456}}",
		"compression", "gzip",
	})
	if err != nil {
		t.Fatalf("parsing error %s", err)
	}

	if header.MinLonE7 != -180*10000000 {
		t.Fatalf(`expected min lon`)
	}
	if header.MinLatE7 != -85*10000000 {
		t.Fatalf(`expected min lat`)
	}
	if header.MaxLonE7 != 180*10000000 {
		t.Fatalf(`expected max lon`)
	}
	if header.MaxLatE7 != 85*10000000 {
		t.Fatalf(`expected max lat`)
	}
	if header.TileType != Mvt {
		t.Fatalf(`expected tile type mvt`)
	}
	if header.CenterLonE7 != -122.1906*10000000 {
		t.Fatalf(`expected center lon`)
	}
	if header.CenterLatE7 != 37.7599*10000000 {
		t.Fatalf(`expected center lat`)
	}
	if header.CenterZoom != 11 {
		t.Fatalf(`expected center zoom`)
	}
	if header.MinZoom != 1 {
		t.Fatalf(`expected min zoom`)
	}
	if header.MaxZoom != 2 {
		t.Fatalf(`expected max zoom`)
	}
	if header.TileCompression != Gzip {
		t.Fatalf(`expected tile compression`)
	}

	// assert removal of redundant fields
	if _, ok := json_metadata["center"]; ok {
		t.Fatalf(`expected no center in json`)
	}
	if _, ok := json_metadata["bounds"]; ok {
		t.Fatalf(`expected no bounds in json`)
	}

	// assert preservation of metadata fields for roundtrip
	if _, ok := json_metadata["name"]; !ok {
		t.Fatalf(`expected name in json`)
	}
	if _, ok := json_metadata["format"]; !ok {
		t.Fatalf(`expected format in json`)
	}
	if _, ok := json_metadata["attribution"]; !ok {
		t.Fatalf(`expected attribution in json`)
	}
	if _, ok := json_metadata["description"]; !ok {
		t.Fatalf(`expected description in json`)
	}
	if _, ok := json_metadata["type"]; !ok {
		t.Fatalf(`expected type in json`)
	}
	if _, ok := json_metadata["version"]; !ok {
		t.Fatalf(`expected version in json`)
	}
	if _, ok := json_metadata["compression"]; !ok {
		t.Fatalf(`expected compression in json`)
	}

	// assert well-known json fields at top level
	if _, ok := json_metadata["vector_layers"]; !ok {
		t.Fatalf(`expected vector_layers in json`)
	}

	if _, ok := json_metadata["tilestats"]; !ok {
		t.Fatalf(`expected tilestats in json`)
	}
}
