package pmtiles

import (
	"fmt"
	"bytes"
	"encoding/json"
	"compress/gzip"
	// "github.com/schollz/progressbar/v3"
	"io"
	"log"
	"os"
)

func Cluster(logger *log.Logger, InputPMTiles string) error {
	file, _ := os.OpenFile(InputPMTiles, os.O_RDONLY, 0666)
	defer file.Close()

	buf := make([]byte, 127)
	_, _ = file.Read(buf)

	header, _ := deserializeHeader(buf)

	if (header.Clustered) {
		return fmt.Errorf("Archive is already clustered.")
	}

	fmt.Println("total directory size", header.RootLength + header.LeafDirectoryLength)

	metadataReader := io.NewSectionReader(file, int64(header.MetadataOffset), int64(header.MetadataLength))
	var metadataBytes []byte
	if header.InternalCompression == Gzip {
		r, _ := gzip.NewReader(metadataReader)
		metadataBytes, _ = io.ReadAll(r)
	} else {
		metadataBytes, _ = io.ReadAll(metadataReader)
	}
	var parsedMetadata map[string]interface{}
	_ = json.Unmarshal(metadataBytes, &parsedMetadata)

	var CollectEntries func(uint64, uint64, func(EntryV3))

	CollectEntries = func(dir_offset uint64, dir_length uint64, f func(EntryV3)) {
		data, _ := io.ReadAll(io.NewSectionReader(file, int64(dir_offset), int64(dir_length)))

		directory := deserializeEntries(bytes.NewBuffer(data))
		for _, entry := range directory {
			if entry.RunLength > 0 {
				f(entry)
			} else {
				CollectEntries(header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length), f)
			}
		}
	}

	resolver := newResolver(true, false)
	tmpfile, _ := os.CreateTemp("", "pmtiles")

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		data, _ := io.ReadAll(io.NewSectionReader(file, int64(header.TileDataOffset + e.Offset), int64(e.Length)))
		if isNew, newData := resolver.AddTileIsNew(e.TileID, data); isNew {
			tmpfile.Write(newData)
		}
	});

	header.Clustered = true
	_ = finalize(logger, resolver, header, tmpfile, "output.pmtiles", parsedMetadata)
	return nil
}
