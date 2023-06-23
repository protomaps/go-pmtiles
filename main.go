package main

import (
	"fmt"
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

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
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
		Path           string `arg:"" help:"Local path or bucket prefix"`
		Port           int    `default:8080`
		Cors           string `help:"Value of HTTP CORS header."`
		CacheSize      int    `default:64 help:"Size of cache in Megabytes."`
		Bucket         string `help:"Remote bucket"`
		PublicHostname string `help:"Public hostname of tile endpoint e.g. https://example.com"`
	} `cmd:"" help:"Run an HTTP proxy server for Z/X/Y tiles."`

	Upload struct {
		Input          string `arg:"" type:"existingfile"`
		Key            string `arg:""`
		MaxConcurrency int    `default:2 help:"# of upload threads"`
		Bucket         string `required:"" help:"Bucket to upload to."`
	} `cmd:"" help:"Upload a local archive to remote storage."`

	Version struct {
	} `cmd:"" help:"Show the program version."`
}

func main() {
	if len(os.Args) < 2 {
		os.Args = append(os.Args, "--help")
	}

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
	case "serve <path>":
		server, err := pmtiles.NewServer(cli.Serve.Bucket, cli.Serve.Path, logger, cli.Serve.CacheSize, cli.Serve.Cors, cli.Serve.PublicHostname)

		if err != nil {
			logger.Fatalf("Failed to create new server, %v", err)
		}

		server.Start()

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			status_code, headers, body := server.Get(r.Context(), r.URL.Path)
			for k, v := range headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(status_code)
			w.Write(body)
			logger.Printf("served %s in %s", r.URL.Path, time.Since(start))
		})

		logger.Printf("Serving %s %s on port %d with Access-Control-Allow-Origin: %s\n", cli.Serve.Bucket, cli.Serve.Path, cli.Serve.Port, cli.Serve.Cors)
		logger.Fatal(http.ListenAndServe(":"+strconv.Itoa(cli.Serve.Port), nil))
	case "extract <input> <output>":
		logger.Fatalf("This command is not yet implemented.")
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
	case "verify <input>":
		logger.Fatalf("This command is not yet implemented.")
	case "version":
		fmt.Printf("pmtiles %s, commit %s, built at %s\n", version, commit, date)
	default:
		panic(ctx.Command())
	}

}
