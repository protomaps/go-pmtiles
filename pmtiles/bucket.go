package pmtiles

import (
	"context"
	"fmt"
	"gocloud.dev/blob"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type Bucket interface {
	Close() error
	NewRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
}

type HttpBucket struct {
	baseURL string
}

func (b HttpBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	reqURL := b.baseURL + "/" + key

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	return resp.Body, nil
}

func (b HttpBucket) Close() error {
	return nil
}

type BucketAdapter struct {
	Bucket *blob.Bucket
}

func (ba BucketAdapter) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	reader, err := ba.Bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		return nil, err
	}
	return reader, nil
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
		} else {
			if prefix != "" {
				abs, err := filepath.Abs(prefix)
				if err != nil {
					return "", "", err
				}
				return "file://" + abs, key, nil
			}
			abs, err := filepath.Abs(key)
			if err != nil {
				return "", "", err
			}
			return "file://" + filepath.Dir(abs), filepath.Base(abs), nil
		}
	}

	if strings.HasPrefix(bucket, "s3") {
		u, err := url.Parse(bucket)
		if err != nil {
			fmt.Println("Error parsing URL:", err)
			return "", "", err
		}
		values := u.Query()
		values.Set("awssdk", "v2")
		u.RawQuery = values.Encode()
		return u.String(), key, nil
	}
	return bucket, key, nil
}

func OpenBucket(ctx context.Context, bucketURL string, bucketPrefix string) (Bucket, error) {
	if strings.HasPrefix(bucketURL, "http") {
		bucket := HttpBucket{bucketURL}
		return bucket, nil
	} else {
		bucket, err := blob.OpenBucket(ctx, bucketURL)
		if bucketPrefix != "/" && bucketPrefix != "." {
			bucket = blob.PrefixedBucket(bucket, path.Clean(bucketPrefix)+string(os.PathSeparator))
		}
		wrapped_bucket := BucketAdapter{bucket}
		return wrapped_bucket, err
	}
}
