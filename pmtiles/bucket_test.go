package pmtiles

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestNormalizeLocalFile(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("", "", "../foo/bar.pmtiles")
	assert.Equal(t, "bar.pmtiles", key)
	assert.True(t, strings.HasSuffix(bucket, "/foo"))
	assert.True(t, strings.HasPrefix(bucket, "file://"))
}

func TestNormalizeHttp(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("", "", "http://example.com/foo/bar.pmtiles")
	assert.Equal(t, "bar.pmtiles", key)
	assert.Equal(t, "http://example.com/foo", bucket)
}

func TestNormalizeAwsSdkVersion(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("s3://mybucket?awssdk=v1&endpoint=https://foo.bar", "", "abc")
	assert.Equal(t, "abc", key)
	assert.Equal(t, "s3://mybucket?awssdk=v2&endpoint=https%3A%2F%2Ffoo.bar", bucket)
}
func TestNormalizePathPrefixServer(t *testing.T) {
	bucket, key, _ := NormalizeBucketKey("", "../foo", "")
	assert.Equal(t, "", key)
	fmt.Println(bucket)
	assert.True(t, strings.HasSuffix(bucket, "/foo"))
	assert.True(t, strings.HasPrefix(bucket, "file://"))
}
