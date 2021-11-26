package main

import (
	"flag"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"log"
	"os"
	"net/http"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	
	port := flag.String("p", "8080", "port to serve on")
	var cors string
	var cacheSize int
	flag.StringVar(&cors, "cors", "", "CORS allowed origin value")
	flag.IntVar(&cacheSize, "cache", 64, "Cache size in mb")
	flag.Parse()

	command := flag.Arg(0)
	path := flag.Arg(1)

	if (command == "serve") {
		if path == "" {
			logger.Println("USAGE: go-pmtiles LOCAL_PATH or https://BUCKET")
			os.Exit(1)
		}

		loop := pmtiles.NewLoop(path, logger, cacheSize, cors)
		loop.Start()

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			status_code, headers, body := loop.Get(r.URL.Path)
			for k, v := range headers {
				w.Header().Set(k,v)
			}
			w.WriteHeader(status_code)
			w.Write(body)
		})

		logger.Printf("Serving %s on HTTP port: %s\n", path, *port)
		logger.Fatal(http.ListenAndServe(":"+*port, nil))
	} else {
		logger.Println("Unrecognized command.")
		os.Exit(1)
	}
}
