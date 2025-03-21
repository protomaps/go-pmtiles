package pmtiles

import (
	"fmt"
	"github.com/schollz/progressbar/v3"
	"io"
	"log"
	"os"
)

func Cluster(logger *log.Logger, InputPMTiles string, deduplicate bool) error {
	file, err := os.OpenFile(InputPMTiles, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	buf := make([]byte, 127)
	_, err = file.Read(buf)
	if err != nil {
		return err
	}

	header, err := DeserializeHeader(buf)
	if err != nil {
		return err
	}

	if header.Clustered {
		return fmt.Errorf("archive is already clustered")
	}

	fmt.Println("total directory size", header.RootLength+header.LeafDirectoryLength)

	metadataReader := io.NewSectionReader(file, int64(header.MetadataOffset), int64(header.MetadataLength))

	metadata, err := DeserializeMetadata(metadataReader, header.InternalCompression)

	resolver := newResolver(deduplicate, false)
	tmpfile, err := os.CreateTemp("", "pmtiles")
	if err != nil {
		return err
	}

	bar := progressbar.Default(int64(header.TileEntriesCount))

	err = IterateEntries(header,
		func(offset uint64, length uint64) ([]byte, error) {
			return io.ReadAll(io.NewSectionReader(file, int64(offset), int64(length)))
		},
		func(e EntryV3) {
			data, _ := io.ReadAll(io.NewSectionReader(file, int64(header.TileDataOffset+e.Offset), int64(e.Length)))
			if isNew, newData := resolver.AddTileIsNew(e.TileID, data, e.RunLength); isNew {
				tmpfile.Write(newData)
			}
			bar.Add(1)
		})

	if err != nil {
		return err
	}

	file.Close()

	header.Clustered = true
	newHeader, err := finalize(logger, resolver, header, tmpfile, InputPMTiles, metadata)
	if err != nil {
		return err
	}
	fmt.Printf("total directory size %d (%f%% of original)\n", newHeader.RootLength+newHeader.LeafDirectoryLength, float64(newHeader.RootLength+newHeader.LeafDirectoryLength)/float64(header.RootLength+header.LeafDirectoryLength)*100)
	return nil
}
