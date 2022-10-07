package pmtiles

import (
	"context"
	"flag"
	"github.com/schollz/progressbar/v3"
	"gocloud.dev/blob"
	"io"
	"log"
	"os"
)

func Upload(logger *log.Logger, args []string) {
	cmd := flag.NewFlagSet("upload", flag.ExitOnError)
	buffer_size := cmd.Int("buffer-size", 8, "Upload chunk size in megabytes")
	max_concurrency := cmd.Int("max-concurrency", 5, "Number of upload threads")

	cmd.Parse(args)
	file := cmd.Arg(0)
	bucketURL := cmd.Arg(1)

	if file == "" || bucketURL == "" {
		logger.Println("USAGE: upload [-buffer-size B] [-max-concurrency M] INPUT s3://BUCKET?region=region")
		os.Exit(1)
	}

	logger.Println(file, bucketURL)
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Fatalf("Failed to setup bucket: %s", err)
	}
	defer b.Close()

	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("Failed to open file: %s", err)
	}
	defer f.Close()
	filestat, err := f.Stat()
	if err != nil {
		log.Fatalf("Failed to open file: %s", err)
	}
	bar := progressbar.Default(filestat.Size())

	nChunks := int64(0)
	buffer := make([]byte, 16*1024*1024)

	opts := &blob.WriterOptions{
		BufferSize:     *buffer_size * 1000 * 1000,
		MaxConcurrency: *max_concurrency,
	}

	w, err := b.NewWriter(ctx, file, opts)
	if err != nil {
		log.Fatalf("Failed to obtain writer: %s", err)
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
			log.Fatalf("Failed to write to bucket: %s", err)
		}
		bar.Add(n)

		if err != nil && err != io.EOF {
			logger.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		log.Fatalf("Failed to close: %s", err)
	}
}
