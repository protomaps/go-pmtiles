package pmtiles

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/fileblob"
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
	data, etag, status, err := bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 100, 3, "")
	assert.Equal(t, "", mock.request.Header.Get("If-Match"))
	assert.Equal(t, "bytes=100-102", mock.request.Header.Get("Range"))
	assert.Equal(t, "http://tiles.example.com/tiles/a/b/c", mock.request.URL.String())
	assert.Equal(t, 200, status)
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
	data, etag, status, err := bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 0, 3, "etag1")
	assert.Equal(t, "etag1", mock.request.Header.Get("If-Match"))
	assert.Equal(t, 200, status)
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
	_, _, status, err := bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 0, 3, "etag1")
	assert.Equal(t, "etag1", mock.request.Header.Get("If-Match"))
	assert.Equal(t, 412, status)
	assert.True(t, isRefreshRequiredError(err))

	mock.response.StatusCode = 416
	_, _, status, err = bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 0, 3, "etag1")
	assert.Equal(t, 416, status)
	assert.True(t, isRefreshRequiredError(err))

	mock.response.StatusCode = 404
	_, _, status, err = bucket.NewRangeReaderEtag(context.Background(), "a/b/c", 0, 3, "etag1")
	assert.False(t, isRefreshRequiredError(err))
	assert.Equal(t, 404, status)
}

func TestFileBucketReplace(t *testing.T) {
	tmp := t.TempDir()
	bucketURL, _, err := NormalizeBucketKey("", tmp, "")
	assert.Nil(t, err)
	bucket, err := OpenBucket(context.Background(), bucketURL, "")
	assert.Nil(t, err)
	assert.NotNil(t, bucket)
	assert.Nil(t, os.WriteFile(filepath.Join(tmp, "archive.pmtiles"), []byte{1, 2, 3}, 0666))

	// first read from file
	reader, etag1, status, err := bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 1, 1, "")
	assert.Equal(t, 206, status)
	assert.Nil(t, err)
	data, err := io.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, data)

	// change file, verify etag changes
	assert.Nil(t, os.WriteFile(filepath.Join(tmp, "archive.pmtiles"), []byte{4, 5, 6, 7}, 0666))
	reader, etag2, status, err := bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 1, 1, "")
	assert.Equal(t, 206, status)
	assert.Nil(t, err)
	data, err = io.ReadAll(reader)
	assert.Nil(t, err)
	assert.NotEqual(t, etag1, etag2)
	assert.Equal(t, []byte{5}, data)

	// and requesting with old etag fails with refresh required error
	_, _, status, err = bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 1, 1, etag1)
	assert.Equal(t, 412, status)
	assert.True(t, isRefreshRequiredError(err))
}

func TestFileBucketRename(t *testing.T) {
	tmp := t.TempDir()
	assert.Nil(t, os.WriteFile(filepath.Join(tmp, "archive.pmtiles"), []byte{1, 2, 3}, 0666))
	assert.Nil(t, os.WriteFile(filepath.Join(tmp, "archive2.pmtiles"), []byte{4, 5, 6, 7}, 0666))

	bucketURL, _, err := NormalizeBucketKey("", tmp, "")
	assert.Nil(t, err)
	bucket, err := OpenBucket(context.Background(), bucketURL, "")
	assert.Nil(t, err)
	assert.NotNil(t, bucket)
	assert.Nil(t, os.WriteFile(filepath.Join(tmp, "archive.pmtiles"), []byte{1, 2, 3}, 0666))

	// first read from file
	reader, etag1, status, err := bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 1, 1, "")
	assert.Equal(t, 206, status)
	assert.Nil(t, err)
	data, err := io.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, data)

	// change file, verify etag changes
	os.Rename(filepath.Join(tmp, "archive.pmtiles"), filepath.Join(tmp, "archive3.pmtiles"))
	os.Rename(filepath.Join(tmp, "archive2.pmtiles"), filepath.Join(tmp, "archive.pmtiles"))
	reader, etag2, status, err := bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 1, 1, "")
	assert.Equal(t, 206, status)
	assert.Nil(t, err)
	data, err = io.ReadAll(reader)
	assert.Nil(t, err)
	assert.NotEqual(t, etag1, etag2)
	assert.Equal(t, []byte{5}, data)

	// and requesting with old etag fails with refresh required error
	_, _, status, err = bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 1, 1, etag1)
	assert.Equal(t, 412, status)
	assert.True(t, isRefreshRequiredError(err))
}

func TestFileShorterThan16K(t *testing.T) {
	tmp := t.TempDir()
	assert.Nil(t, os.WriteFile(filepath.Join(tmp, "archive.pmtiles"), []byte{1, 2, 3}, 0666))

	bucketURL, _, err := NormalizeBucketKey("", tmp, "")
	bucket, err := OpenBucket(context.Background(), bucketURL, "")

	reader, _, status, err := bucket.NewRangeReaderEtag(context.Background(), "archive.pmtiles", 0, 16384, "")
	assert.Equal(t, 206, status)
	assert.Nil(t, err)
	data, err := io.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(data))
}
