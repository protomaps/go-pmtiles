package pmtiles

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"gocloud.dev/blob"
)

// Bucket is an abstration over a gocloud or plain HTTP bucket.
type Bucket interface {
	Close() error
	NewRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
	NewRangeReaderEtag(ctx context.Context, key string, offset int64, length int64, etag string) (io.ReadCloser, string, error)
}

// RefreshRequiredError is an error that indicates the etag has chanced on the remote file
type RefreshRequiredError struct {
	StatusCode int
}

func (m *RefreshRequiredError) Error() string {
	return fmt.Sprintf("HTTP error indicates file has changed: %d", m.StatusCode)
}

type mockBucket struct {
	items map[string][]byte
}

func (m mockBucket) Close() error {
	return nil
}

func (m mockBucket) NewRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	body, _, err := m.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err

}
func (m mockBucket) NewRangeReaderEtag(_ context.Context, key string, offset int64, length int64, etag string) (io.ReadCloser, string, error) {
	bs, ok := m.items[key]
	if !ok {
		return nil, "", fmt.Errorf("Not found %s", key)
	}

	hash := md5.Sum(bs)
	resultEtag := hex.EncodeToString(hash[:])
	if len(etag) > 0 && resultEtag != etag {
		return nil, "", &RefreshRequiredError{}
	}
	if offset+length > int64(len(bs)) {
		return nil, "", &RefreshRequiredError{416}
	}

	return io.NopCloser(bytes.NewReader(bs[offset:(offset + length)])), resultEtag, nil
}

// HTTPClient is an interface that lets you swap out the default client with a mock one in tests
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPBucket struct {
	baseURL string
	client  HTTPClient
}

func (b HTTPBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (b HTTPBucket) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, error) {
	reqURL := b.baseURL + "/" + key

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, "", err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	if len(etag) > 0 {
		req.Header.Set("If-Match", etag)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, "", err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		if isRefreshRequredCode(resp.StatusCode) {
			err = &RefreshRequiredError{resp.StatusCode}
		} else {
			err = fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}
		return nil, "", err
	}

	return resp.Body, resp.Header.Get("ETag"), nil
}

func (b HTTPBucket) Close() error {
	return nil
}

func isRefreshRequredCode(code int) bool {
	return code == http.StatusPreconditionFailed || code == http.StatusRequestedRangeNotSatisfiable
}

type BucketAdapter struct {
	Bucket *blob.Bucket
}

func (ba BucketAdapter) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, err := ba.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (ba BucketAdapter) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, error) {
	reader, err := ba.Bucket.NewRangeReader(ctx, key, offset, length, &blob.ReaderOptions{
		BeforeRead: func(asFunc func(interface{}) bool) error {
			var req *s3.GetObjectInput
			if len(etag) > 0 && asFunc(&req) {
				req.IfMatch = &etag
			}
			return nil
		},
	})
	if err != nil {
		var resp awserr.RequestFailure
		errors.As(err, &resp)
		if resp != nil && isRefreshRequredCode(resp.StatusCode()) {
			return nil, "", &RefreshRequiredError{resp.StatusCode()}
		}
		return nil, "", err
	}
	resultETag := ""
	var resp s3.GetObjectOutput
	if reader.As(&resp) {
		resultETag = *resp.ETag
	}
	return reader, resultETag, nil
}

func (ba BucketAdapter) Close() error {
	return ba.Bucket.Close()
}

func NormalizeBucketKey(bucket string, prefix string, key string) (string, string, error) {
	if bucket == "" {
		if strings.HasPrefix(key, "http") {
			u, err := url.Parse(key)
			if err != nil {
				return "", "", err
			}
			dir, file := path.Split(u.Path)
			if strings.HasSuffix(dir, "/") {
				dir = dir[:len(dir)-1]
			}
			return u.Scheme + "://" + u.Host + dir, file, nil
		}
		fileprotocol := "file://"
		if string(os.PathSeparator) != "/" {
			fileprotocol += "/"
		}
		if prefix != "" {
			abs, err := filepath.Abs(prefix)
			if err != nil {
				return "", "", err
			}
			return fileprotocol + filepath.ToSlash(abs), key, nil
		}
		abs, err := filepath.Abs(key)
		if err != nil {
			return "", "", err
		}
		return fileprotocol + filepath.ToSlash(filepath.Dir(abs)), filepath.Base(abs), nil
	}
	return bucket, key, nil
}

func OpenBucket(ctx context.Context, bucketURL string, bucketPrefix string) (Bucket, error) {
	if strings.HasPrefix(bucketURL, "http") {
		bucket := HTTPBucket{bucketURL, http.DefaultClient}
		return bucket, nil
	}
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}
	if bucketPrefix != "" && bucketPrefix != "/" && bucketPrefix != "." {
		bucket = blob.PrefixedBucket(bucket, path.Clean(bucketPrefix)+string(os.PathSeparator))
	}
	wrappedBucket := BucketAdapter{bucket}
	return wrappedBucket, err
}
