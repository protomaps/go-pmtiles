package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func TestNormalizeLocalFile(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("", "", "../foo/bar.pmtiles")
	assert.Equal(t, "bar.pmtiles", key)
	assert.True(t, strings.HasSuffix(bucket, "/foo"))
	assert.True(t, strings.HasPrefix(bucket, "file://"))
}

func TestNormalizeLocalFileWindows(t *testing.T) {
	if string(os.PathSeparator) != "/" {
		bucket, key, _ := NormalizeBucketKey("", "", "\\foo\\bar.pmtiles")
		assert.Equal(t, "bar.pmtiles", key)
		assert.True(t, strings.HasSuffix(bucket, "/foo"))
		assert.True(t, strings.HasPrefix(bucket, "file://"))
	}
}

func TestNormalizeHttp(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("", "", "http://example.com/foo/bar.pmtiles")
	assert.Equal(t, "bar.pmtiles", key)
	assert.Equal(t, "http://example.com/foo", bucket)
}

func TestNormalizePathPrefixServer(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("", "../foo", "")
	assert.Equal(t, "", key)
	assert.True(t, strings.HasSuffix(bucket, "/foo"))
	assert.True(t, strings.HasPrefix(bucket, "file://"))
}
