package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRawPolygonRegion(t *testing.T) {
	result, err := UnmarshalRegion([]byte(`{
		"type": "Polygon",
		"coordinates": [[[0, 0],[0,1],[1,1],[0,0]]]
	}`))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
}

func TestRawMultiPolygonRegion(t *testing.T) {
	result, err := UnmarshalRegion([]byte(`{
		"type": "MultiPolygon",
		"coordinates": [[[[0, 0],[0,1],[1,1],[0,0]]]]
	}`))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
}

func TestRawPolygonFeatureRegion(t *testing.T) {
	result, err := UnmarshalRegion([]byte(`{
		"type": "Feature",
		"geometry": {
			"type": "Polygon",
			"coordinates": [[[0, 0],[0,1],[1,1],[0,0]]]
		}
	}`))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
}

func TestRawMultiPolygonFeatureRegion(t *testing.T) {
	result, err := UnmarshalRegion([]byte(`{
		"type": "Feature",
		"geometry": {
			"type": "MultiPolygon",
			"coordinates": [[[[0, 0],[0,1],[1,1],[0,0]]]]
		}
	}`))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
}
