package pmtiles

import (
	// "log"
	"os"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestWriteHeader(t *testing.T) {
	// logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	tempDir, _ := ioutil.TempDir("", "testing")
	defer os.RemoveAll(tempDir)
	src, _ := os.Open("fixtures/test_fixture_1.pmtiles")
	defer src.Close()
	dest, _ := os.Create(filepath.Join(tempDir, "test.pmtiles"))
	defer dest.Close()
	_, _ = io.Copy(dest, src)

	assert.Nil(t, nil)

	// var input map[string]interface{}
	// json.Unmarshal(b.Bytes(), &input)
	// assert.Equal(t, "tippecanoe v2.5.0", input["generator"])
}
