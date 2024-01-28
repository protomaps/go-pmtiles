package pmtiles

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTilejson(t *testing.T) {
	// Define test inputs
	header := HeaderV3{
		MinZoom:     0.0,
		MaxZoom:     14.0,
		MinLonE7:    -1144000000,
		MinLatE7:    479000000,
		MaxLonE7:    -1139000000,
		MaxLatE7:    483000000,
		CenterLonE7: -1141500000,
		CenterLatE7: 481000000,
		TileType:    Mvt,
	}
	metadataBytes := []byte(`
	{
		"vector_layers": [{"id": "layer1"}],
		"attribution": "Attribution",
		"description": "Description",
		"name": "Name",
		"version": "1.0"
	}`)
	tileURL := "https://example.com/foo"

	// Call the function
	tilejsonBytes, err := CreateTileJSON(header, metadataBytes, tileURL)

	// Check for errors
	if err != nil {
		t.Errorf("CreateTilejson returned an error: %v", err)
	}

	// Parse the tilejsonBytes to check the output
	var tilejson map[string]interface{}
	err = json.Unmarshal(tilejsonBytes, &tilejson)
	if err != nil {
		t.Errorf("Failed to parse the generated TileJSON: %v", err)
	}

	assert.Equal(t, "3.0.0", tilejson["tilejson"])
	assert.Equal(t, "xyz", tilejson["scheme"])
	assert.Equal(t, []interface{}{"https://example.com/foo/{z}/{x}/{y}.mvt"}, tilejson["tiles"])
	assert.Equal(t, []interface{}{map[string]interface{}{"id": "layer1"}}, tilejson["vector_layers"])
	assert.Equal(t, "Attribution", tilejson["attribution"])
	assert.Equal(t, "Description", tilejson["description"])
	assert.Equal(t, "Name", tilejson["name"])
	assert.Equal(t, "1.0", tilejson["version"])

	assert.Equal(t, []interface{}{-114.400000, 47.900000, -113.900000, 48.300000}, tilejson["bounds"])
	assert.Equal(t, []interface{}{-114.150000, 48.100000, 0.0}, tilejson["center"])
	assert.Equal(t, 0.0, tilejson["minzoom"])
	assert.Equal(t, 14.0, tilejson["maxzoom"])
}

func TestCreateTilejsonOptionalFields(t *testing.T) {
	header := HeaderV3{
		MinZoom:     0.0,
		MaxZoom:     14.0,
		MinLonE7:    -1144000000,
		MinLatE7:    479000000,
		MaxLonE7:    -1139000000,
		MaxLatE7:    483000000,
		CenterLonE7: -1141500000,
		CenterLatE7: 481000000,
		TileType:    Png,
	}
	metadataBytes := []byte(`
	{
	}`)

	tilejsonBytes, err := CreateTileJSON(header, metadataBytes, "")

	// Check for errors
	if err != nil {
		t.Errorf("CreateTilejson returned an error: %v", err)
	}

	var tilejson map[string]interface{}
	err = json.Unmarshal(tilejsonBytes, &tilejson)
	if err != nil {
		t.Errorf("Failed to parse the generated TileJSON: %v", err)
	}

	assert.Equal(t, "3.0.0", tilejson["tilejson"])
	assert.Equal(t, "xyz", tilejson["scheme"])
	assert.Equal(t, []interface{}{"https://example.com/{z}/{x}/{y}.png"}, tilejson["tiles"])
	assert.NotContains(t, tilejson, "attribution")
	assert.NotContains(t, tilejson, "description")
	assert.NotContains(t, tilejson, "name")
	assert.NotContains(t, tilejson, "version")
}
