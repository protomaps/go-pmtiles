package pmtiles

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func GetHeaderMetadata(ctx context.Context, bucket Bucket, key string) (error, HeaderV3, []byte) {
	header_bytes, err := bucket.NewRangeReader(ctx, key, 0, 4096)
	if err != nil {
		return err, HeaderV3{}, nil
	}
	defer header_bytes.Close()

	// TODO internal compression gzip? -- see server get_header_metadata
	b, err := io.ReadAll(header_bytes)
	if err != nil {
		return err, HeaderV3{}, nil
	}

	// TODO look at server implemntation of reading headers?
	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])
	if err != nil {
		// check to see if it's a V2 file
		if string(b[0:2]) == "PM" {
			spec_version := b[2]
			return fmt.Errorf("PMTiles version %d detected; please use 'pmtiles convert' to upgrade to version 3.", spec_version), HeaderV3{}, nil
		}

		return err, HeaderV3{}, nil
	}

	// TODO read error handling?
	// TODO what should we be reading here
	var metadata_bytes []byte
	if header.InternalCompression == Gzip {
		metadata_reader, _ := gzip.NewReader(r)
		defer metadata_reader.Close()
		metadata_bytes, err = io.ReadAll(metadata_reader)
	} else if header.InternalCompression == NoCompression {
		metadata_bytes, err = io.ReadAll(r)
	} else {
		return errors.New("Unknown compression"), HeaderV3{}, nil
	}

	return nil, header, metadata_bytes
}

func GetTilejson(ctx context.Context, bucket Bucket, key string, tileUrl string) ([]byte, error) {
	tilejson := make(map[string]interface{})

	// TOOD public hostname
	// tileUrl = []string{server.publicHostname + "/" + name}

	err, header, metadata_bytes := GetHeaderMetadata(ctx, bucket, key)
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
