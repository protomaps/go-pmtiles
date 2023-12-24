package pmtiles

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTilejson(t *testing.T) {
	// Define test inputs
	header := HeaderV3{
		// Define header fields
	}
	metadataBytes := []byte(`
	{
		"vector_layers": [{"id": "layer1"}],
		"attribution": "Attribution",
		"description": "Description",
		"name": "Name",
		"version": "1.0"
	}`)
	tileURL := "https://example.com/tiles/{z}/{x}/{y}.pbf"

	// Call the function
	tilejsonBytes, err := CreateTilejson(header, metadataBytes, tileURL)

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

	// Add assertions to validate the generated TileJSON
	// For example:
	// if tilejson.Name != "My Tileset" {
	// 	t.Errorf("Unexpected tileset name. Expected: %s, Got: %s", "My Tileset", tilejson.Name)
	// }

	// Add more test cases as needed
}
