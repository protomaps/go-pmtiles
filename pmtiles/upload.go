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

// Determine the multipart block size based on the total file size.
func partSizeBytes(totalSize int64) int {
	if totalSize/(5*1024*1024) >= 10_000 {
		return int(totalSize/10_000 + 1)
	}
	return 5 * 1024 * 1024
}

// Upload a pmtiles archive to a bucket.
func Upload(_ *log.Logger, InputPMTiles string, bucket string, RemotePMTiles string, maxConcurrency int) error {
	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("Failed to setup bucket: %w", err)
	}
	defer b.Close()

	f, err := os.Open(InputPMTiles)
	if err != nil {
		return fmt.Errorf("Failed to open file: %w", err)
	}
	defer f.Close()

	filestat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("Failed to stat file: %w", err)
	}

	opts := &blob.WriterOptions{
		BufferSize:     partSizeBytes(filestat.Size()),
		MaxConcurrency: maxConcurrency,
	}

	w, err := b.NewWriter(ctx, RemotePMTiles, opts)
	if err != nil {
		return fmt.Errorf("Failed to obtain writer: %w", err)
	}

	bar := progressbar.Default(filestat.Size())
	io.Copy(io.MultiWriter(w, bar), f)

	if err := w.Close(); err != nil {
		return fmt.Errorf("Failed to complete upload: %w", err)
	}

	return nil
}
