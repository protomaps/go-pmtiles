package pmtiles

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

func GetHeaderMetadata(ctx context.Context, bucket Bucket, key string) (HeaderV3, error) {
	header_bytes, err := bucket.NewRangeReader(ctx, key, 0, 4096)
	if err != nil {
		return HeaderV3{}, err
	}
	defer header_bytes.Close()

	// TODO internal compression gzip? -- see server get_header_metadata
	b, err := io.ReadAll(header_bytes)
	if err != nil {
		return HeaderV3{}, err
	}

	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])
	if err != nil {
		// check to see if it's a V2 file
		if string(b[0:2]) == "PM" {
			spec_version := b[2]
			return HeaderV3{}, fmt.Errorf("PMTiles version %d detected; please use 'pmtiles convert' to upgrade to version 3.", spec_version)
		}

		return HeaderV3{}, err
	}

	return header, nil
}

func GetTilejson(ctx context.Context, bucket Bucket, key string, tileUrl string) ([]byte, error) {
	tilejson := make(map[string]interface{})

	// TOOD public hostname
	// tileUrl = []string{server.publicHostname + "/" + name}

	header, err := GetHeaderMetadata(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	var metadata_map map[string]interface{}
	json.Unmarshal(metadata_bytes, &metadata_map)

	tilejson["tilejson"] = "3.0.0"
	tilejson["scheme"] = "xyz"
	tilejson["tiles"] = []string{tileUrl + "/{z}/{x}/{y}" + headerExt(header)}
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

	return json.Marshal(tilejson)
}
