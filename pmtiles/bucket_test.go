package pmtiles

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

type ClientMock struct {
	request  *http.Request
	response *http.Response
}

func (c *ClientMock) Do(req *http.Request) (*http.Response, error) {
	c.request = req
	return c.response, nil
}

func TestHttpBucketRequestNormal(t *testing.T) {
	mock := ClientMock{}
	header := http.Header{}
	header.Add("ETag", "etag")
	bucket := HTTPBucket{"http://tiles.example.com/tiles", &mock}
	mock.response = &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("abc")),
		Header:     header,
	}
	data, etag, err := bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 100, 3, "")
	assert.Equal(t, "", mock.request.Header.Get("If-Match"))
	assert.Equal(t, "bytes=100-102", mock.request.Header.Get("Range"))
	assert.Equal(t, "http://tiles.example.com/tiles/a/b/c", mock.request.URL.String())
	assert.Nil(t, err)
	b, err := io.ReadAll(data)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(b))
	assert.Equal(t, "etag", etag)
	assert.Nil(t, err)
}

func TestHttpBucketRequestRequestEtag(t *testing.T) {
	mock := ClientMock{}
	header := http.Header{}
	header.Add("ETag", "etag2")
	bucket := HTTPBucket{"http://tiles.example.com/tiles", &mock}
	mock.response = &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("abc")),
		Header:     header,
	}
	data, etag, err := bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 0, 3, "etag1")
	assert.Equal(t, "etag1", mock.request.Header.Get("If-Match"))
	assert.Nil(t, err)
	b, err := io.ReadAll(data)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(b))
	assert.Equal(t, "etag2", etag)
	assert.Nil(t, err)
}

func TestHttpBucketRequestRequestEtagFailed(t *testing.T) {
	mock := ClientMock{}
	header := http.Header{}
	header.Add("ETag", "etag2")
	bucket := HTTPBucket{"http://tiles.example.com/tiles", &mock}
	mock.response = &http.Response{
		StatusCode: 412,
		Body:       io.NopCloser(strings.NewReader("abc")),
		Header:     header,
	}
	_, _, err := bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 0, 3, "etag1")
	assert.Equal(t, "etag1", mock.request.Header.Get("If-Match"))
	assert.True(t, isRefreshRequredError(err))
}
