package main

import (
	"flag"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	if len(os.Args) < 2 {
		logger.Println("Command is required.")
		os.Exit(1)
	}

	command := os.Args[1]

	if command == "serve" {
		port := flag.String("p", "8080", "port to serve on")
		var cors string
		var cacheSize int
		flag.StringVar(&cors, "cors", "", "CORS allowed origin value")
		flag.IntVar(&cacheSize, "cache", 64, "Cache size in mb")
		flag.Parse()
		path := flag.Arg(1)

		if path == "" {
			logger.Println("USAGE: go-pmtiles serve LOCAL_PATH or https://BUCKET")
			os.Exit(1)
		}

		loop := pmtiles.NewLoop(path, logger, cacheSize, cors)
		loop.Start()

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			status_code, headers, body := loop.Get(r.URL.Path)
			for k, v := range headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(status_code)
			w.Write(body)
		})

		logger.Printf("Serving %s on HTTP port: %s\n", path, *port)
		logger.Fatal(http.ListenAndServe(":"+*port, nil))
	} else if command == "subpyramid" {
		flag.Parse()
		path := flag.Arg(1)
		output := flag.Arg(2)

		var err error
		num_args := make([]int, 5)
		for i := 0; i < 5; i++ {
			if num_args[i], err = strconv.Atoi(flag.Arg(i + 3)); err != nil {
				panic(err)
			}
		}
		pmtiles.Subpyramid(logger, path, output, uint8(num_args[0]), uint32(num_args[1]), uint32(num_args[2]), uint32(num_args[3]), uint32(num_args[4]))
	} else {
		logger.Println("Unrecognized command.")
		os.Exit(1)
	}
}
