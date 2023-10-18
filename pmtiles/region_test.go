package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBboxRegion(t *testing.T) {
	result, err := BboxRegion("-1.906033,50.680367,1.097501,52.304934")
	assert.Nil(t, err)
	assert.Equal(t, -1.906033, result[0][0][0][0])
	assert.Equal(t, 52.304934, result[0][0][0][1])
	assert.Equal(t, 1.097501, result[0][0][2][0])
	assert.Equal(t, 50.680367, result[0][0][2][1])
}

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

func TestFeatureCollectionRegion(t *testing.T) {
	result, err := UnmarshalRegion([]byte(`{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"geometry": {
					"type": "MultiPolygon",
					"coordinates": [[[[0, 0],[0,1],[1,1],[0,0]]]]
				}
			},
			{
				"type": "Feature",
				"geometry": {
					"type": "Polygon",
					"coordinates": [[[1, 1],[1,2],[2,2],[1,1]]]
				}
			}
		]
	}`))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
}

func TestEmptyFeatureCollectionRegion(t *testing.T) {
	_, err := UnmarshalRegion([]byte(`{
		"type": "FeatureCollection",
		"features": [
		]
	}`))
	assert.NotNil(t, err)
}
