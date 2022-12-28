package main

import (
	"github.com/alecthomas/kong"
	"github.com/protomaps/go-pmtiles/pmtiles"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var cli struct {
	Convert struct {
		Input           string `arg:"" help:"Input archive." type:"path"`
		Output          string `arg:"" help:"Output PMTiles archive." type:"path"`
		Force           bool   `help:"Force removal."`
		NoDeduplication bool   `help:"Don't attempt to deduplicate tiles."`
		Tmpdir          string `help:"An optional path to a folder for tmp data." type:"existingdir"`
	} `cmd:"" help:"Convert an MBTiles or older spec version to PMTiles."`

	Show struct {
		Bucket string `arg:"" help:"Remote bucket or local file:// dir"`
		Key    string `arg:""`
		Z      *int   `arg:"" optional:""`
		X      *int   `arg:"" optional:""`
		Y      *int   `arg:"" optional:""`
	} `cmd:"" help:"Inspect a local dir (file://) or remote (s3://) PMTiles."`

	Serve struct {
		Bucket    string `arg:""`
		Port      int    `default:8080`
		Cors      string `help:"Value of HTTP CORS header."`
		CacheSize int    `default:64 help:"Size of cache in Megabytes."`
	} `cmd:"" help:"Run an HTTP proxy server for Z/X/Y tiles."`

	Upload struct {
		Input          string `arg:""`
		Bucket         string `arg:""`
		Key            string `arg:""`
		MaxConcurrency int    `default:2 help:"# of upload threads"`
	} `cmd:"" help:"Upload a local PMTiles to remote storage."`
}

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	ctx := kong.Parse(&cli)

	switch ctx.Command() {
	case "show <bucket> <key>":
		err := pmtiles.Show(logger, cli.Show.Bucket, cli.Show.Key, cli.Show.Z, cli.Show.X, cli.Show.Y)
		if err != nil {
			logger.Fatalf("Failed to show database, %v", err)
		}
	case "show <bucket> <key> <z> <x> <y>":
		err := pmtiles.Show(logger, cli.Show.Bucket, cli.Show.Key, cli.Show.Z, cli.Show.X, cli.Show.Y)
		if err != nil {
			logger.Fatalf("Failed to show database, %v", err)
		}
	case "serve <bucket>":
		loop, err := pmtiles.NewLoop(cli.Serve.Bucket, logger, cli.Serve.Port, cli.Serve.Cors)

		if err != nil {
			logger.Fatalf("Failed to create new loop, %v", err)
		}

		loop.Start()

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			status_code, headers, body := loop.Get(r.Context(), r.URL.Path)
			for k, v := range headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(status_code)
			w.Write(body)
			logger.Printf("served %s in %s", r.URL.Path, time.Since(start))
		})

		logger.Printf("Serving %s on HTTP port: %s with Access-Control-Allow-Origin: %s\n", cli.Serve.Bucket, cli.Serve.Port, cli.Serve.Cors)
		logger.Fatal(http.ListenAndServe(":"+strconv.Itoa(cli.Serve.Port), nil))
	case "extract <input>":
		// not yet implemented
	case "convert <input> <output>":
		path := cli.Convert.Input
		output := cli.Convert.Output

		var tmpfile *os.File

		if cli.Convert.Tmpdir == "" {
			var err error
			tmpfile, err = os.CreateTemp("", "pmtiles")

			if err != nil {
				logger.Fatalf("Failed to create temp file, %w", err)
			}
		} else {
			abs_tmproot, err := filepath.Abs(cli.Convert.Tmpdir)

			if err != nil {
				logger.Fatalf("Failed to derive absolute path for %s, %v", cli.Convert.Tmpdir, err)
			}

			info, err := os.Stat(abs_tmproot)

			if err != nil {
				logger.Fatalf("Failed to stat %s, %v", abs_tmproot, err)
			}

			if !info.IsDir() {
				logger.Fatalf("%s is not a directory", abs_tmproot)
			}

			tmpfile, err = os.CreateTemp(abs_tmproot, "pmtiles")

			if err != nil {
				logger.Fatalf("Failed to create temp file, %w", err)
			}
		}

		defer os.Remove(tmpfile.Name())
		err := pmtiles.Convert(logger, path, output, !cli.Convert.NoDeduplication, tmpfile)

		if err != nil {
			logger.Fatalf("Failed to convert %s, %v", path, err)
		}
	case "upload <input> <bucket> <key>":

		err := pmtiles.Upload(logger, cli.Upload.Input, cli.Upload.Bucket, cli.Upload.Key, cli.Upload.MaxConcurrency)

		if err != nil {
			logger.Fatalf("Failed to upload file, %v", err)
		}
	case "validate <path>":
		// not yet implemented
	default:
		panic(ctx.Command())
	}

}
