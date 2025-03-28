package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/protomaps/go-pmtiles/pmtiles"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

var cli struct {
	Show struct {
		Path       string `arg:""`
		Bucket     string `help:"Remote bucket"`
		Metadata   bool   `help:"Print only the JSON metadata"`
		HeaderJson bool   `help:"Print a JSON representation of part of the header information"`
		Tilejson   bool   `help:"Print the TileJSON"`
		PublicURL  string `help:"Public base URL of tile endpoint for TileJSON e.g. https://example.com/tiles"`
	} `cmd:"" help:"Inspect a local or remote archive"`

	Tile struct {
		Path   string `arg:""`
		Z      int    `arg:""`
		X      int    `arg:""`
		Y      int    `arg:""`
		Bucket string `help:"Remote bucket"`
	} `cmd:"" help:"Fetch one tile from a local or remote archive and output on stdout"`

	Cluster struct {
		Input           string `arg:"" help:"Input archive" type:"existingfile"`
		NoDeduplication bool   `help:"Don't attempt to deduplicate tiles"`
	} `cmd:"" help:"Cluster an unclustered local archive, optimizing the size and layout"`

	Edit struct {
		Input      string `arg:"" help:"Input archive" type:"existingfile"`
		HeaderJson string `help:"Input header JSON file (written by show --header-json)" type:"existingfile"`
		Metadata   string `help:"Input metadata JSON (written by show --metadata)" type:"existingfile"`
	} `cmd:"" help:"Edit JSON metadata or parts of the header"`

	Extract struct {
		Input           string  `arg:"" help:"Input local or remote archive"`
		Output          string  `arg:"" help:"Output archive" type:"path"`
		Bucket          string  `help:"Remote bucket of input archive"`
		Region          string  `help:"local GeoJSON Polygon or MultiPolygon file for area of interest" type:"existingfile"`
		Bbox            string  `help:"bbox area of interest: min_lon,min_lat,max_lon,max_lat" type:"string"`
		Minzoom         int8    `default:"-1" help:"Minimum zoom level, inclusive"`
		Maxzoom         int8    `default:"-1" help:"Maximum zoom level, inclusive"`
		DownloadThreads int     `default:"4" help:"Number of download threads"`
		DryRun          bool    `help:"Calculate tiles to extract, but don't download them"`
		Overfetch       float32 `default:"0.05" help:"What ratio of extra data to download to minimize # requests; 0.2 is 20%"`
	} `cmd:"" help:"Create an archive from a larger archive for a subset of zoom levels or geographic region"`

	Merge struct {
		Output string   `arg:"" help:"Output archive" type:"path"`
		Input  []string `arg:"" help:"Input archives"`
	} `cmd:"" help:"Merge multiple archives into a single archive" hidden:""`

	Convert struct {
		Input           string `arg:"" help:"Input archive" type:"existingfile"`
		Output          string `arg:"" help:"Output archive" type:"path"`
		Force           bool   `help:"Force removal"`
		NoDeduplication bool   `help:"Don't attempt to deduplicate tiles"`
		Tmpdir          string `help:"An optional path to a folder for temporary files" type:"existingdir"`
	} `cmd:"" help:"Convert an MBTiles database to PMTiles"`

	Verify struct {
		Input string `arg:"" help:"Input archive" type:"existingfile"`
	} `cmd:"" help:"Verify the correctness of an archive structure, without verifying individual tile contents"`

	Makesync struct {
		Input       string `arg:"" type:"existingfile"`
		BlockSizeKb int    `default:"20" help:"The approximate block size, in kilobytes; 0 means 1 tile = 1 block"`
	} `cmd:"" help:"" hidden:""`

	Sync struct {
		Existing         string `arg:"" type:"existingfile"`
		New              string `arg:"" help:"Local or remote archive, with .sync sidecar file"`
		DryRun           bool   `help:"Calculate new parts to download, but don't download them"`
		RangesPerRequest int    `default:"100" help:"Number of ranges in a single HTTP request (limit depends on server)"`
	} `cmd:"" help:"Sync a local file with a remote one by only downloading changed parts" hidden:""`

	Serve struct {
		Path      string `arg:"" help:"Local path or bucket prefix"`
		Interface string `default:"0.0.0.0"`
		Port      int    `default:"8080"`
		AdminPort int    `default:"-1"`
		Cors      string `help:"Comma-separated list of of allowed HTTP CORS origins"`
		CacheSize int    `default:"64" help:"Size of cache in megabytes"`
		Bucket    string `help:"Remote bucket"`
		PublicURL string `help:"Public base URL of tile endpoint for TileJSON e.g. https://example.com/tiles/"`
	} `cmd:"" help:"Run an HTTP proxy server for Z/X/Y tiles"`

	Upload struct {
		InputPmtiles   string `arg:"" type:"existingfile" help:"The local PMTiles file"`
		RemotePmtiles  string `arg:""  help:"The name for the remote PMTiles source"`
		MaxConcurrency int    `default:"2" help:"# of upload threads"`
		Bucket         string `required:"" help:"Bucket to upload to"`
	} `cmd:"" help:"Upload a local archive to remote storage"`

	Version struct {
	} `cmd:"" help:"Show the program version"`
}

func main() {
	if len(os.Args) < 2 {
		os.Args = append(os.Args, "--help")
	}

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	ctx := kong.Parse(&cli)

	switch ctx.Command() {
	case "show <path>":
		err := pmtiles.Show(logger, os.Stdout, cli.Show.Bucket, cli.Show.Path, cli.Show.HeaderJson, cli.Show.Metadata, cli.Show.Tilejson, cli.Show.PublicURL, false, 0, 0, 0)
		if err != nil {
			logger.Fatalf("Failed to show archive, %v", err)
		}
	case "tile <path> <z> <x> <y>":
		err := pmtiles.Show(logger, os.Stdout, cli.Tile.Bucket, cli.Tile.Path, false, false, false, "", true, cli.Tile.Z, cli.Tile.X, cli.Tile.Y)
		if err != nil {
			logger.Fatalf("Failed to show tile, %v", err)
		}
	case "serve <path>":
		server, err := pmtiles.NewServer(cli.Serve.Bucket, cli.Serve.Path, logger, cli.Serve.CacheSize, cli.Serve.PublicURL)

		if err != nil {
			logger.Fatalf("Failed to create new server, %v", err)
		}

		pmtiles.SetBuildInfo(version, commit, date)
		server.Start()

		mux := http.NewServeMux()

		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			statusCode := server.ServeHTTP(w, r)
			logger.Printf("served %d %s in %s", statusCode, url.PathEscape(r.URL.Path), time.Since(start))
		})

		logger.Printf("Serving %s %s on port %d and interface %s with Access-Control-Allow-Origin: %s\n", cli.Serve.Bucket, cli.Serve.Path, cli.Serve.Port, cli.Serve.Interface, cli.Serve.Cors)
		if cli.Serve.AdminPort > 0 {
			go func() {
				adminPort := strconv.Itoa(cli.Serve.AdminPort)
				logger.Printf("Serving /metrics on port %s and interface %s\n", adminPort, cli.Serve.Interface)
				adminMux := http.NewServeMux()
				adminMux.Handle("/metrics", promhttp.Handler())
				logger.Fatal(startHTTPServer(cli.Serve.Interface+":"+adminPort, adminMux))
			}()
		}

		if cli.Serve.Cors != "" {
			muxWithCors := pmtiles.NewCors(cli.Serve.Cors).Handler(mux)
			logger.Fatal(startHTTPServer(cli.Serve.Interface+":"+strconv.Itoa(cli.Serve.Port), muxWithCors))
		} else {
			logger.Fatal(startHTTPServer(cli.Serve.Interface+":"+strconv.Itoa(cli.Serve.Port), mux))
		}
	case "extract <input> <output>":
		err := pmtiles.Extract(logger, cli.Extract.Bucket, cli.Extract.Input, cli.Extract.Minzoom, cli.Extract.Maxzoom, cli.Extract.Region, cli.Extract.Bbox, cli.Extract.Output, cli.Extract.DownloadThreads, cli.Extract.Overfetch, cli.Extract.DryRun)
		if err != nil {
			logger.Fatalf("Failed to extract, %v", err)
		}
	case "cluster <input>":
		err := pmtiles.Cluster(logger, cli.Cluster.Input, !cli.Cluster.NoDeduplication)
		if err != nil {
			logger.Fatalf("Failed to cluster, %v", err)
		}
	case "convert <input> <output>":
		path := cli.Convert.Input
		output := cli.Convert.Output

		var tmpfile *os.File

		if cli.Convert.Tmpdir == "" {
			var err error
			tmpfile, err = os.CreateTemp("", "pmtiles")

			if err != nil {
				logger.Fatalf("Failed to create temp file, %v", err)
			}
		} else {
			absTemproot, err := filepath.Abs(cli.Convert.Tmpdir)

			if err != nil {
				logger.Fatalf("Failed to derive absolute path for %s, %v", cli.Convert.Tmpdir, err)
			}

			tmpfile, err = os.CreateTemp(absTemproot, "pmtiles")

			if err != nil {
				logger.Fatalf("Failed to create temp file, %v", err)
			}
		}

		defer os.Remove(tmpfile.Name())
		err := pmtiles.Convert(logger, path, output, !cli.Convert.NoDeduplication, tmpfile)

		if err != nil {
			logger.Fatalf("Failed to convert %s, %v", path, err)
		}
	case "upload <input-pmtiles> <remote-pmtiles>":
		err := pmtiles.Upload(logger, cli.Upload.InputPmtiles, cli.Upload.Bucket, cli.Upload.RemotePmtiles, cli.Upload.MaxConcurrency)

		if err != nil {
			logger.Fatalf("Failed to upload file, %v", err)
		}
	case "verify <input>":
		err := pmtiles.Verify(logger, cli.Verify.Input)
		if err != nil {
			logger.Fatalf("Failed to verify archive, %v", err)
		}
	case "edit <input>":
		err := pmtiles.Edit(logger, cli.Edit.Input, cli.Edit.HeaderJson, cli.Edit.Metadata)
		if err != nil {
			logger.Fatalf("Failed to edit archive, %v", err)
		}
	case "makesync <input>":
		err := pmtiles.Makesync(logger, version, cli.Makesync.Input, cli.Makesync.BlockSizeKb)
		if err != nil {
			logger.Fatalf("Failed to makesync archive, %v", err)
		}
	case "sync <existing> <new>":
		err := pmtiles.Sync(logger, cli.Sync.Existing, cli.Sync.New, cli.Sync.DryRun)
		if err != nil {
			logger.Fatalf("Failed to sync archive, %v", err)
		}
	case "version":
		fmt.Printf("pmtiles %s, commit %s, built at %s\n", version, commit, date)
	default:
		panic(ctx.Command())
	}

}
func startHTTPServer(addr string, handler http.Handler) error {
	server := &http.Server{
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		Addr:              addr,
		Handler:           handler,
	}
	return server.ListenAndServe()
}
