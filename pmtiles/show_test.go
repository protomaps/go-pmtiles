package pmtiles

import (
	"fmt"
	"path"
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestShowHeader(t *testing.T) {
	var b bytes.Buffer
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	wd, _ := os.Getwd()
	fmt.Println(wd)
	err := Show(logger, &b, "", path.Join(wd, "fixtures/test_fixture_1.pmtiles"), true, false, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "mvt", input["TileType"])
	assert.Equal(t, "gzip", input["TileCompression"])
}

func TestShowMetadata(t *testing.T) {
	var b bytes.Buffer
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	wd, _ := os.Getwd()
	err := Show(logger, &b, "", path.Join(wd,"fixtures/test_fixture_1.pmtiles"), false, true, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "tippecanoe v2.5.0", input["generator"])
}
