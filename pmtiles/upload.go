package pmtiles

import (
	"context"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"gocloud.dev/blob"
	"io"
	"log"
	"os"
)

func Upload(logger *log.Logger, input string, bucket string, key string, max_concurrency int) error {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("Failed to setup bucket: %w", err)
	}
	defer b.Close()

	f, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("Failed to open file: %w", err)
	}
	defer f.Close()
	filestat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("Failed to open file: %w", err)
	}
	bar := progressbar.Default(filestat.Size())

	nChunks := int64(0)
	buffer := make([]byte, 8*1024)

	opts := &blob.WriterOptions{
		BufferSize:     256 * 1024 * 1024,
		MaxConcurrency: max_concurrency,
	}

	w, err := b.NewWriter(ctx, key, opts)
	if err != nil {
		return fmt.Errorf("Failed to obtain writer: %w", err)
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
			return fmt.Errorf("Failed to write to bucket: %w", err)
		}
		bar.Add(n)

		if err != nil && err != io.EOF {
			return fmt.Errorf("Failed to write data, %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("Failed to close: %w", err)
	}

	return nil
}
