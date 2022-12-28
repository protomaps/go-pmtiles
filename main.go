package main

import (
	"flag"
	"fmt"
	"github.com/protomaps/go-pmtiles/pmtiles"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	if len(os.Args) < 2 {
		helptext := `Usage: pmtiles [COMMAND] [ARGS]

Inspecting pmtiles:
pmtiles show file:// INPUT.pmtiles
pmtiles show "s3://BUCKET_NAME INPUT.pmtiles

Creating pmtiles:
pmtiles convert INPUT.mbtiles OUTPUT.pmtiles
pmtiles convert INPUT_V2.pmtiles OUTPUT_V3.pmtiles

Uploading pmtiles:
pmtiles upload INPUT.pmtiles s3://BUCKET_NAME REMOTE.pmtiles

Running a proxy server:
pmtiles serve "s3://BUCKET_NAME"`
		fmt.Println(helptext)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "show":
		err := pmtiles.Show(logger, os.Args[2:])

		if err != nil {
			logger.Fatalf("Failed to show database, %v", err)
		}
	case "serve":
		serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)
		port := serveCmd.String("p", "8080", "port to serve on")
		cors := serveCmd.String("cors", "", "CORS allowed origin value")
		cacheSize := serveCmd.Int("cache", 64, "Cache size in mb")
		serveCmd.Parse(os.Args[2:])
		path := serveCmd.Arg(0)
		if path == "" {
			logger.Println("USAGE: serve  [-p PORT] [-cors VALUE] LOCAL_PATH or https://BUCKET")
			os.Exit(1)
		}
		loop, err := pmtiles.NewLoop(path, logger, *cacheSize, *cors)

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

		logger.Printf("Serving %s on HTTP port: %s with Access-Control-Allow-Origin: %s\n", path, *port, *cors)
		logger.Fatal(http.ListenAndServe(":"+*port, nil))
	case "subpyramid":
		subpyramidCmd := flag.NewFlagSet("subpyramid", flag.ExitOnError)
		cpuProfile := subpyramidCmd.Bool("profile", false, "profiling output")
		subpyramidCmd.Parse(os.Args[2:])
		path := subpyramidCmd.Arg(0)
		output := subpyramidCmd.Arg(1)

		var err error
		num_args := make([]int, 5)
		for i := 0; i < 5; i++ {
			if num_args[i], err = strconv.Atoi(subpyramidCmd.Arg(i + 2)); err != nil {
				panic(err)
			}
		}

		if *cpuProfile {
			f, err := os.Create("output.profile")
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
		bounds := "-180,-90,180,90" // TODO deal with antimeridian, center of tile, etc
		pmtiles.SubpyramidXY(logger, path, output, uint8(num_args[0]), uint32(num_args[1]), uint32(num_args[2]), uint32(num_args[3]), uint32(num_args[4]), bounds)
	case "convert":
		convertCmd := flag.NewFlagSet("convert", flag.ExitOnError)
		no_deduplication := convertCmd.Bool("no-deduplication", false, "Don't deduplicate data")
		tmproot := convertCmd.String("tmpdir", "", "An optional path to a folder to write tmp data to")
		convertCmd.Parse(os.Args[2:])
		path := convertCmd.Arg(0)
		output := convertCmd.Arg(1)

		if *tmproot == "" {
			err := pmtiles.Convert(logger, path, output, !(*no_deduplication))

			if err != nil {
				logger.Fatalf("Failed to convert %s, %v", path, err)
			}

		} else {

			abs_tmproot, err := filepath.Abs(*tmproot)

			if err != nil {
				logger.Fatalf("Failed to derive absolute path for %s, %v", tmproot, err)
			}

			info, err := os.Stat(abs_tmproot)

			if err != nil {
				logger.Fatalf("Failed to stat %s, %v", abs_tmproot, err)
			}

			if !info.IsDir() {
				logger.Fatalf("%s is not a directory", abs_tmproot)
			}

			now := time.Now()
			tmpname := fmt.Sprintf("convert-%d", now.UnixMilli())

			tmpfile := filepath.Join(abs_tmproot, tmpname)

			f, err := os.OpenFile(tmpfile, os.O_RDWR|os.O_CREATE, 0644)

			if err != nil {
				logger.Fatalf("Failed to open %s for writing, %v", tmpfile, err)
			}

			defer os.Remove(tmpfile)

			err = pmtiles.ConvertWithTempFile(logger, path, output, !(*no_deduplication), f)

			if err != nil {
				logger.Fatalf("Failed to convert %s, %v", path, err)
			}

		}

	case "upload":
		err := pmtiles.Upload(logger, os.Args[2:])

		if err != nil {
			logger.Fatalf("Failed to upload file, %v", err)
		}

	case "validate":
		// pmtiles.Validate()
	default:
		logger.Println("unrecognized command.")
		flag.PrintDefaults()
		os.Exit(1)
	}

}
