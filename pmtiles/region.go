package pmtiles

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"strconv"
	"strings"
)

// BboxRegion parses a bbox string into an orb.MultiPolygon region.
func BboxRegion(bbox string) (orb.MultiPolygon, error) {
	parts := strings.Split(bbox, ",")
	minLon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return nil, err
	}
	minLat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, err
	}
	maxLon, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nil, err
	}
	maxLat, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}
	return orb.MultiPolygon{{{{minLon, maxLat}, {maxLon, maxLat}, {maxLon, minLat}, {minLon, minLat}, {minLon, maxLat}}}}, nil
}

// UnmarshalRegion parses JSON bytes into an orb.MultiPolygon region.
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
