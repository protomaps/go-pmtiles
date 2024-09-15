package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteHeader(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "testing")
	defer os.RemoveAll(tempDir)
	src, _ := os.Open("fixtures/test_fixture_1.pmtiles")
	defer src.Close()
	dest, _ := os.Create(filepath.Join(tempDir, "test.pmtiles"))
	defer dest.Close()
	_, _ = io.Copy(dest, src)
}
