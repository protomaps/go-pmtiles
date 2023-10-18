package pmtiles

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"strconv"
	"strings"
)

func BboxRegion(bbox string) (orb.MultiPolygon, error) {
	parts := strings.Split(bbox, ",")
	min_lon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return nil, err
	}
	min_lat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, err
	}
	max_lon, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nil, err
	}
	max_lat, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}
	return orb.MultiPolygon{{{{min_lon, max_lat}, {max_lon, max_lat}, {max_lon, min_lat}, {min_lon, min_lat}, {min_lon, max_lat}}}}, nil
}

func UnmarshalRegion(data []byte) (orb.MultiPolygon, error) {
	fc, err := geojson.UnmarshalFeatureCollection(data)

	if err == nil {
		retval := make([]orb.Polygon, 0)
		for _, f := range fc.Features {
			switch v := f.Geometry.(type) {
			case orb.Polygon:
				retval = append(retval, v)
			case orb.MultiPolygon:
				retval = append(retval, v...)
			}
		}
		if len(retval) > 0 {
			return retval, nil
		}
	}

	f, err := geojson.UnmarshalFeature(data)

	if err == nil {
		switch v := f.Geometry.(type) {
		case orb.Polygon:
			return []orb.Polygon{v}, nil
		case orb.MultiPolygon:
			return v, nil
		}
	}

	g, err := geojson.UnmarshalGeometry(data)

	if err != nil {
		return nil, err
	}

	switch v := g.Geometry().(type) {
	case orb.Polygon:
		return []orb.Polygon{v}, nil
	case orb.MultiPolygon:
		return v, nil
	}

	return nil, fmt.Errorf("No geometry")
}
