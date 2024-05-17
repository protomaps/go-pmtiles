package pmtiles

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestShowHeader(t *testing.T) {
	var b bytes.Buffer
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	err := Show(logger, &b, "", "fixtures/test_fixture_1.pmtiles", true, false, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "mvt", input["TileType"])
}

func TestShowMetadata(t *testing.T) {
	var b bytes.Buffer
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	err := Show(logger, &b, "", "fixtures/test_fixture_1.pmtiles", false, true, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "tippecanoe v2.5.0", input["generator"])
}
