package pmtiles

import (
	"testing"
)

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

	if header.MinLon != -180.0 {
		t.Fatalf(`expected min lon`)
	}
	if header.MinLat != -85 {
		t.Fatalf(`expected min lat`)
	}
	if header.MaxLon != 180.0 {
		t.Fatalf(`expected max lon`)
	}
	if header.MaxLat != 85.0 {
		t.Fatalf(`expected max lat`)
	}
	if header.TileType != Mvt {
		t.Fatalf(`expected tile type mvt`)
	}
	if header.CenterLon != -122.1906 {
		t.Fatalf(`expected center lon`)
	}
	if header.CenterLat != 37.7599 {
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
