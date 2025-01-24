package pmtiles

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

func makeFixtureCopy(t *testing.T, name string) string {
	src, _ := os.OpenFile("fixtures/test_fixture_1.pmtiles", os.O_RDONLY, 0666)
	defer src.Close()
	fname := filepath.Join(t.TempDir(), name+".pmtiles")
	dest, _ := os.Create(fname)
	_, _ = io.Copy(dest, src)
	dest.Close()
	return fname
}

func TestEditHeader(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_header")

	headerPath := filepath.Join(t.TempDir(), "header.json")
	headerFile, _ := os.Create(headerPath)
	fmt.Fprint(headerFile, `{"tile_type":"png","tile_compression":"br","bounds":[-1,1,-1,1],"center":[0,0,0]}`)
	headerFile.Close()

	err := Edit(logger, fileToEdit, headerPath, "")
	assert.Nil(t, err)

	var b bytes.Buffer
	err = Show(logger, &b, "", fileToEdit, true, false, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "png", input["tile_type"])
	assert.Equal(t, "br", input["tile_compression"])
}

func TestEditMetadata(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_metadata")

	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	metadataFile, _ := os.Create(metadataPath)
	fmt.Fprint(metadataFile, `{"foo":"bar"}`)
	metadataFile.Close()

	err := Edit(logger, fileToEdit, "", metadataPath)
	assert.Nil(t, err)

	var b bytes.Buffer
	err = Show(logger, &b, "", fileToEdit, false, true, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "bar", input["foo"])
}

func TestMalformedHeader(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_header_malformed")

	headerPath := filepath.Join(t.TempDir(), "header_malformed.json")
	headerFile, _ := os.Create(headerPath)
	fmt.Fprint(headerFile, `{`)
	headerFile.Close()

	err := Edit(logger, fileToEdit, headerPath, "")
	assert.Error(t, err)
}

func TestMalformedHeaderBounds(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_header_bad_bounds")

	headerPath := filepath.Join(t.TempDir(), "header_bad_bounds.json")
	headerFile, _ := os.Create(headerPath)
	fmt.Fprint(headerFile, `{"tile_type":"png","tile_compression":"br","bounds":[],"center":[0,0,0]}`)
	headerFile.Close()

	err := Edit(logger, fileToEdit, headerPath, "")
	assert.Error(t, err)
}

func TestHeaderUnknownEnum(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_header_unknown")

	headerPath := filepath.Join(t.TempDir(), "header_unknown.json")
	headerFile, _ := os.Create(headerPath)
	fmt.Fprint(headerFile, `{"tile_type":"foo","tile_compression":"foo","bounds":[-1,1,-1,1],"center":[0,0,0]}`)
	headerFile.Close()

	err := Edit(logger, fileToEdit, headerPath, "")
	assert.Nil(t, err)

	var b bytes.Buffer
	err = Show(logger, &b, "", fileToEdit, true, false, false, "", false, 0, 0, 0)
	assert.Nil(t, err)

	var input map[string]interface{}
	json.Unmarshal(b.Bytes(), &input)
	assert.Equal(t, "", input["tile_type"])
	assert.Equal(t, "unknown", input["tile_compression"])
}

func TestMalformedHeaderCenter(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_header_bad_center")

	headerPath := filepath.Join(t.TempDir(), "header_bad_center.json")
	headerFile, _ := os.Create(headerPath)
	fmt.Fprint(headerFile, `{"tile_type":"png","tile_compression":"br","bounds":[-1,1,-1,1],"center":[0,0]}`)
	headerFile.Close()

	err := Edit(logger, fileToEdit, headerPath, "")
	assert.Error(t, err)
}

func TestMalformedMetadata(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_metadata_malformed")

	metadataPath := filepath.Join(t.TempDir(), "metadata_malformed.json")
	metadataFile, _ := os.Create(metadataPath)
	fmt.Fprint(metadataFile, `{`)
	metadataFile.Close()

	err := Edit(logger, fileToEdit, "", metadataPath)
	assert.Error(t, err)
}

func TestTempfileExists(t *testing.T) {
	fileToEdit := makeFixtureCopy(t, "edit_existing_tempfile")

	tmp, _ := os.Create(fileToEdit + ".tmp")
	defer tmp.Close()

	metadataPath := filepath.Join(t.TempDir(), "metadata.json")
	metadataFile, _ := os.Create(metadataPath)
	fmt.Fprint(metadataFile, `{"foo":"bar"}`)
	metadataFile.Close()

	err := Edit(logger, fileToEdit, "", metadataPath)
	assert.Error(t, err)
}
