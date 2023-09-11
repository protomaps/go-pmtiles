package pmtiles

import (
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

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
