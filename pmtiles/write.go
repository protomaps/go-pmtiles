package pmtiles

import (
	"fmt"
	"log"
	"os"
)

func Write(logger *log.Logger, inputArchive string, newHeaderJsonFile string, newMetadataFile string) error {
	if newMetadataFile == "" {
		if newHeaderJsonFile == "" {
			return fmt.Errorf("No data to write.")
		}
		// we can write the header in-place without writing the whole file.
		return nil
	}

	// write metadata:
	// always writes in this order:
	// copy the header
	// copy the root directory
	// write the new the metadata
	// copy the leaf directories
	// copy the tile data
	file, err := os.OpenFile(inputArchive, os.O_RDWR, 0666)

	buf := make([]byte, 127)
	_, err = file.Read(buf)
	if err != nil {
		return err
	}
	originalHeader, _ := deserializeHeader(buf)

	// modify the header

	buf = serializeHeader(originalHeader)
	_, err = file.WriteAt(buf, 0)
	if err != nil {
		return err
	}
	return nil
}
