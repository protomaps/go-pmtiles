package main

import (
	"log"
	"os"
	
	"github.com/protomaps/go-pmtiles/app/pmtiles"	
)


func main() {

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	err := pmtiles.Run(logger)

	if err != nil {
		logger.Fatal(err)
	}
}
