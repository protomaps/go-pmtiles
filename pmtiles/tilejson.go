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
	tilejson["tiles"] = []string{tileUrl}
	tilejson["vector_layers"] = metadata_map["vector_layers"]
	tilejson["attribution"] = metadata_map["attribution"]
	tilejson["description"] = metadata_map["description"]
	tilejson["name"] = metadata_map["name"]
	tilejson["version"] = metadata_map["version"]

	E7 := 10000000.0
	tilejson["bounds"] = []float64{float64(header.MinLonE7) / E7, float64(header.MinLatE7) / E7, float64(header.MaxLonE7) / E7, float64(header.MaxLatE7) / E7}
	tilejson["center"] = []interface{}{float64(header.CenterLonE7) / E7, float64(header.CenterLatE7) / E7, header.CenterZoom}
	tilejson["minzoom"] = header.MinZoom
	tilejson["maxzoom"] = header.MaxZoom

	return json.MarshalIndent(tilejson, "", "\t")
}
