package pmtiles

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/everFinance/goar"
	"github.com/everFinance/goar/types"
	"github.com/schollz/progressbar/v3"
	"gocloud.dev/blob"
)

const ArweaveScheme = "arweave"

// Upload a pmtiles archive to a bucket.
func Upload(logger *log.Logger, input string, bucket string, key string, maxConcurrency int) error {
	parsedUri, err := url.Parse(bucket)
	if err != nil {
		return fmt.Errorf("unable to parse bucket uri: %w", err)
	}

	switch parsedUri.Scheme {
	case ArweaveScheme:
		return uploadWithArweave(logger, input, parsedUri, key)
	default:
		return uploadWithGoCloud(logger, input, parsedUri, key, maxConcurrency)
	}
}

func uploadWithGoCloud(logger *log.Logger, input string, bucketUri *url.URL, key string, maxConcurrency int) error {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, bucketUri.String())
	if err != nil {
		return fmt.Errorf("unable to setup bucket: %w", err)
	}
	defer b.Close()

	f, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer f.Close()
	filestat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	bar := progressbar.Default(filestat.Size())

	nChunks := int64(0)
	buffer := make([]byte, 8*1024)

	opts := &blob.WriterOptions{
		BufferSize:     256 * 1024 * 1024,
		MaxConcurrency: maxConcurrency,
	}

	w, err := b.NewWriter(ctx, key, opts)
	if err != nil {
		return fmt.Errorf("unable to obtain writer: %w", err)
	}

	for {
		n, err := f.Read(buffer)

		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			logger.Fatal(err)
		}

		nChunks++

		_, err = w.Write(buffer[:n])
		if err != nil {
			return fmt.Errorf("unable to write to bucket: %w", err)
		}
		bar.Add(n)

		if err != nil && err != io.EOF {
			return fmt.Errorf("unable to write data, %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("unable to close: %w", err)
	}

	return nil
}

// uploadWithArweave expects a bucket in the format of arweave://arweave.net/path_to_secret.json?tag=value&otherTag=otherValue
func uploadWithArweave(logger *log.Logger, input string, bucket *url.URL, key string) error {
	data, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("unable to open pmtile file: %w", err)
	}
	defer data.Close()

	arNode := "https://" + bucket.Host
	w, err := goar.NewWalletFromPath(bucket.Path, arNode) // path to private key
	if err != nil {
		return fmt.Errorf("unable to intialize arweave wallet: %w", err)
	}

	tags := []types.Tag{
		{Name: "Content-Type", Value: "application/vnd.pmtiles"},
		{Name: "Unix-Time", Value: fmt.Sprintf("%d", time.Now().Unix())},
	}

	// append any query parameters as tags
	for key, listVals := range bucket.Query() {
		if len(listVals) > 0 {
			tags = append(tags, types.Tag{Name: key, Value: listVals[0]})
		}
	}

	tx, err := w.SendDataStreamSpeedUp(data, tags, 10)
	if err != nil {
		return fmt.Errorf("unable to upload file to arweave: %w", err)
	}

	log.Printf("PMTile will be available soon at ar://%s", tx.ID)
	return nil
}
