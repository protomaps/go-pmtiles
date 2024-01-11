package pmtiles

import (
	"encoding/json"
)

func CreateTilejson(header HeaderV3, metadata_bytes []byte, tileUrl string) ([]byte, error) {
	var metadata_map map[string]interface{}
	json.Unmarshal(metadata_bytes, &metadata_map)

	tilejson := make(map[string]interface{})

	tilejson["tilejson"] = "3.0.0"
	tilejson["scheme"] = "xyz"

	if tileUrl == "" {
		tileUrl = "https://example.com"
	}

	tilejson["tiles"] = []string{tileUrl + "/{z}/{x}/{y}" + headerExt(header)}
	tilejson["vector_layers"] = metadata_map["vector_layers"]

	if val, ok := metadata_map["attribution"]; ok {
		tilejson["attribution"] = val
	}

	if val, ok := metadata_map["description"]; ok {
		tilejson["description"] = val
	}

	if val, ok := metadata_map["name"]; ok {
		tilejson["name"] = val
	}

	if val, ok := metadata_map["version"]; ok {
		tilejson["version"] = val
	}

	E7 := 10000000.0
	tilejson["bounds"] = []float64{float64(header.MinLonE7) / E7, float64(header.MinLatE7) / E7, float64(header.MaxLonE7) / E7, float64(header.MaxLatE7) / E7}
	tilejson["center"] = []interface{}{float64(header.CenterLonE7) / E7, float64(header.CenterLatE7) / E7, header.CenterZoom}
	tilejson["minzoom"] = header.MinZoom
	tilejson["maxzoom"] = header.MaxZoom

	return json.MarshalIndent(tilejson, "", "\t")
}
