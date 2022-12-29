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
		Input           string `arg:"" help:"Input archive." type:"existingfile"`
		Output          string `arg:"" help:"Output PMTiles archive." type:"path"`
		Force           bool   `help:"Force removal."`
		NoDeduplication bool   `help:"Don't attempt to deduplicate tiles."`
		Tmpdir          string `help:"An optional path to a folder for tmp data." type:"existingdir"`
	} `cmd:"" help:"Convert an MBTiles or older spec version to PMTiles."`

	Show struct {
		Path   string `arg:""`
		Bucket string `help:"Remote bucket"`
	} `cmd:"" help:"Inspect a local or remote archive."`

	Tile struct {
		Path   string `arg:""`
		Z      int    `arg:""`
		X      int    `arg:""`
		Y      int    `arg:""`
		Bucket string `help:"Remote bucket"`
	} `cmd:"" help:"Fetch one tile from a local or remote archive and output on stdout."`

	Extract struct {
		Input   string `arg:"" help:"Input local or remote archive."`
		Output  string `arg:"" help:"Output archive." type:"path"`
		Bucket  string `help:"Remote bucket of input archive."`
		Region  string `help:"local GeoJSON Polygon or MultiPolygon file for area of interest." type:"existingfile"`
		Maxzoom int    `help:"Maximum zoom level, inclusive."`
		DryRun  bool   `help:"Calculate tiles to extract based on header and directories, but don't download them."`
	} `cmd:"" help:"Create an archive from a larger archive for a subset of zoom levels or geographic region."`

	Verify struct {
		Input string `arg:"" help:"Input archive." type:"existingfile"`
	} `cmd:"" help:"Verifies that a local archive is valid."`

	Serve struct {
		Dir       string `arg:"" help:"Local path or bucket prefix"`
		Port      int    `default:8080`
		Cors      string `help:"Value of HTTP CORS header."`
		CacheSize int    `default:64 help:"Size of cache in Megabytes."`
		Bucket    string `help:"Remote bucket"`
	} `cmd:"" help:"Run an HTTP proxy server for Z/X/Y tiles."`

	Upload struct {
		Input          string `arg:"" type:"existingfile"`
		Key            string `arg:""`
		MaxConcurrency int    `default:2 help:"# of upload threads"`
		Bucket         string `required:"" help:"Bucket to upload to."`
	} `cmd:"" help:"Upload a local archive to remote storage."`
}

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	ctx := kong.Parse(&cli)

	switch ctx.Command() {
	case "show <path>":
		err := pmtiles.Show(logger, cli.Show.Bucket, cli.Show.Path, false, 0, 0, 0)
		if err != nil {
			logger.Fatalf("Failed to show database, %v", err)
		}
	case "tile <path> <z> <x> <y>":
		err := pmtiles.Show(logger, cli.Tile.Bucket, cli.Tile.Path, true, cli.Tile.Z, cli.Tile.X, cli.Tile.Y)
		if err != nil {
			logger.Fatalf("Failed to show database, %v", err)
		}
	case "serve <dir>":
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

		logger.Printf("Serving %s on port %d with Access-Control-Allow-Origin: %s\n", cli.Serve.Bucket, cli.Serve.Port, cli.Serve.Cors)
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
	case "upload <input> <key>":
		err := pmtiles.Upload(logger, cli.Upload.Input, cli.Upload.Bucket, cli.Upload.Key, cli.Upload.MaxConcurrency)

		if err != nil {
			logger.Fatalf("Failed to upload file, %v", err)
		}
	case "verify <path>":
		// not yet implemented
	default:
		panic(ctx.Command())
	}

}
