package pmtiles

import (
	"bytes"
	"context"
	"encoding/binary"
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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cespare/xxhash/v2"
	"gocloud.dev/blob"
)

// Bucket is an abstration over a gocloud or plain HTTP bucket.
type Bucket interface {
	Close() error
	NewRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
	NewRangeReaderEtag(ctx context.Context, key string, offset int64, length int64, etag string) (io.ReadCloser, string, int, error)
}

// RefreshRequiredError is an error that indicates the etag has changed on the remote file
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
	body, _, _, err := m.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (m mockBucket) NewRangeReaderEtag(_ context.Context, key string, offset int64, length int64, etag string) (io.ReadCloser, string, int, error) {
	bs, ok := m.items[key]
	if !ok {
		return nil, "", 404, fmt.Errorf("Not found %s", key)
	}

	resultEtag := generateEtag(bs)
	if len(etag) > 0 && resultEtag != etag {
		return nil, "", 412, &RefreshRequiredError{}
	}
	if offset >= int64(len(bs)) {
		return nil, "", 416, &RefreshRequiredError{416}
	}

	end := offset + length
	if end > int64(len(bs)) {
		end = int64(len(bs))
	}
	return io.NopCloser(bytes.NewReader(bs[offset:end])), resultEtag, 206, nil
}

// FileBucket is a bucket backed by a directory on disk
type FileBucket struct {
	path string
}

// NewFileBucket initializes a FileBucket and returns a new instance
func NewFileBucket(path string) *FileBucket {
	return &FileBucket{path: path}
}

func (b FileBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func uintToBytes(n uint64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, n)
	return bs
}

func hasherToEtag(hasher *xxhash.Digest) string {
	sum := uintToBytes(hasher.Sum64())
	return fmt.Sprintf(`"%s"`, hex.EncodeToString(sum))
}

func generateEtag(data []byte) string {
	hasher := xxhash.New()
	hasher.Write(data)
	return hasherToEtag(hasher)
}

func generateEtagFromInts(ns ...int64) string {
	hasher := xxhash.New()
	for _, n := range ns {
		hasher.Write(uintToBytes(uint64(n)))
	}
	return hasherToEtag(hasher)
}

func (b FileBucket) NewRangeReaderEtag(_ context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	name := filepath.Join(b.path, key)
	file, err := os.Open(name)
	defer file.Close()
	if err != nil {
		return nil, "", 404, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, "", 404, err
	}
	newEtag := generateEtagFromInts(info.ModTime().UnixNano(), info.Size())
	if len(etag) > 0 && etag != newEtag {
		return nil, "", 412, &RefreshRequiredError{}
	}
	result := make([]byte, length)
	read, err := file.ReadAt(result, offset)

	if err == io.EOF {
		part := result[0:read]
		return io.NopCloser(bytes.NewReader(part)), newEtag, 206, nil
	}

	if err != nil {
		return nil, "", 500, err
	}
	if read != int(length) {
		return nil, "", 416, fmt.Errorf("Expected to read %d bytes but only read %d", length, read)
	}

	return io.NopCloser(bytes.NewReader(result)), newEtag, 206, nil
}

func (b FileBucket) Close() error {
	return nil
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
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (b HTTPBucket) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	reqURL := b.baseURL + "/" + key

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, "", 500, err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	if len(etag) > 0 {
		req.Header.Set("If-Match", etag)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, "", resp.StatusCode, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		if isRefreshRequiredCode(resp.StatusCode) {
			err = &RefreshRequiredError{resp.StatusCode}
		} else {
			err = fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}
		return nil, "", resp.StatusCode, err
	}

	return resp.Body, resp.Header.Get("ETag"), resp.StatusCode, nil
}

func (b HTTPBucket) Close() error {
	return nil
}

func isRefreshRequiredCode(code int) bool {
	return code == http.StatusPreconditionFailed || code == http.StatusRequestedRangeNotSatisfiable
}

// StatusCoder is used to get the actual status code out from a cloud vendor specific error
type StatusCoder interface {
	StatusCodeFromError(error) int
}

// handleReaderResponse is the common main logic for all cloud
func handleReaderResponse(reader *blob.Reader, b StatusCoder, err error, getETag func(interface{}) string) (io.ReadCloser, string, int, error) {
	status := 206
	if err != nil {
		status = 404

		statusCode := b.StatusCodeFromError(err)

		if isRefreshRequiredCode(statusCode) {
			return nil, "", statusCode, &RefreshRequiredError{statusCode}
		}

		return nil, "", status, err
	}

	resultETag := ""
	reader.As(func(resp interface{}) bool {
		resultETag = getETag(resp)
		return true
	})

	return reader, resultETag, status, nil
}

// S3BucketAdapter implements the Bucket interface for S3
type S3BucketAdapter struct {
	*blob.Bucket
}

func (ba S3BucketAdapter) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	reader, err := ba.Bucket.NewRangeReader(ctx, key, offset, length, &blob.ReaderOptions{
		BeforeRead: func(asFunc func(interface{}) bool) error {
			var req *s3.GetObjectInput
			if len(etag) > 0 && asFunc(&req) {
				req.IfMatch = &etag
			}
			return nil
		},
	})
	return handleReaderResponse(reader, ba, err, func(resp interface{}) string {
		if s3Resp, ok := resp.(*s3.GetObjectOutput); ok {
			return *s3Resp.ETag
		}
		return ""
	})
}

func (ba S3BucketAdapter) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := ba.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (ba S3BucketAdapter) Close() error {
	return ba.Bucket.Close()
}

func (ba S3BucketAdapter) StatusCodeFromError(err error) int {
	var resp awserr.RequestFailure
	errors.As(err, &resp)
	status := 404
	if resp != nil {
		status = resp.StatusCode()
	}

	return status
}

type AzureBucketAdapter struct {
	Bucket *blob.Bucket
}

func (ba AzureBucketAdapter) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := ba.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (ba AzureBucketAdapter) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	reader, err := ba.Bucket.NewRangeReader(ctx, key, offset, length, &blob.ReaderOptions{
		BeforeRead: func(asFunc func(interface{}) bool) error {
			var req *azblobblob.DownloadStreamOptions
			if len(etag) > 0 && asFunc(&req) {
				azureEtag := azcore.ETag(etag)
				if req.AccessConditions == nil {
					req.AccessConditions = &azblobblob.AccessConditions{}
				}
				if req.AccessConditions.ModifiedAccessConditions == nil {
					req.AccessConditions.ModifiedAccessConditions = &azblobblob.ModifiedAccessConditions{}
				}
				req.AccessConditions.ModifiedAccessConditions.IfMatch = &azureEtag
			}
			return nil
		},
	})
	return handleReaderResponse(reader, ba, err, func(resp interface{}) string {
		if azureResp, ok := resp.(*azblobblob.DownloadStreamResponse); ok {
			return string(*azureResp.ETag)
		}
		return ""
	})
}

func (ba AzureBucketAdapter) Close() error {
	return ba.Bucket.Close()
}

func (ba AzureBucketAdapter) StatusCodeFromError(err error) int {
	var resp *azcore.ResponseError
	errors.As(err, &resp)
	status := 404
	if resp != nil {
		return resp.StatusCode
	}

	return status
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
	if strings.HasPrefix(bucketURL, "file") {
		fileprotocol := "file://"
		if string(os.PathSeparator) != "/" {
			fileprotocol += "/"
		}
		path := strings.Replace(bucketURL, fileprotocol, "", 1)
		bucket := NewFileBucket(filepath.FromSlash(path))
		return bucket, nil
	}

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}
	if bucketPrefix != "" && bucketPrefix != "/" && bucketPrefix != "." {
		bucket = blob.PrefixedBucket(bucket, path.Clean(bucketPrefix)+string(os.PathSeparator))
	}
	if strings.HasPrefix(bucketURL, "azblob") {
		bucket := AzureBucketAdapter{bucket}
		return bucket, nil
	}
	wrappedBucket := S3BucketAdapter{bucket}
	return wrappedBucket, err
}
