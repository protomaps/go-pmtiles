package pmtiles

import (
	"encoding/json"
)

// CreateTileJSON returns TileJSON from an archive header+metadata and a given public tileURL.
func CreateTileJSON(header HeaderV3, metadataBytes []byte, tileURL string) ([]byte, error) {
	var metadataMap map[string]interface{}
	json.Unmarshal(metadataBytes, &metadataMap)

	tilejson := make(map[string]interface{})

	tilejson["tilejson"] = "3.0.0"
	tilejson["scheme"] = "xyz"

	if tileURL == "" {
		tileURL = "https://example.com"
	}

	tilejson["tiles"] = []string{tileURL + "/{z}/{x}/{y}" + headerExt(header)}
	tilejson["vector_layers"] = metadataMap["vector_layers"]

	if val, ok := metadataMap["attribution"]; ok {
		tilejson["attribution"] = val
	}

	if val, ok := metadataMap["description"]; ok {
		tilejson["description"] = val
	}

	if val, ok := metadataMap["name"]; ok {
		tilejson["name"] = val
	}

	if val, ok := metadataMap["version"]; ok {
		tilejson["version"] = val
	}

	E7 := 10000000.0
	tilejson["bounds"] = []float64{float64(header.MinLonE7) / E7, float64(header.MinLatE7) / E7, float64(header.MaxLonE7) / E7, float64(header.MaxLatE7) / E7}
	tilejson["center"] = []interface{}{float64(header.CenterLonE7) / E7, float64(header.CenterLatE7) / E7, header.CenterZoom}
	tilejson["minzoom"] = header.MinZoom
	tilejson["maxzoom"] = header.MaxZoom

	return json.MarshalIndent(tilejson, "", "\t")
}
