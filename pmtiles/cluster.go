package pmtiles

import (
	"bytes"
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
		return fmt.Errorf("Archive is already clustered.")
	}

	fmt.Println("total directory size", header.RootLength+header.LeafDirectoryLength)

	metadataReader := io.NewSectionReader(file, int64(header.MetadataOffset), int64(header.MetadataLength))

	metadata, err := DeserializeMetadata(metadataReader, header.InternalCompression)

	var CollectEntries func(uint64, uint64, func(EntryV3))

	CollectEntries = func(dir_offset uint64, dir_length uint64, f func(EntryV3)) {
		data, _ := io.ReadAll(io.NewSectionReader(file, int64(dir_offset), int64(dir_length)))

		directory := DeserializeEntries(bytes.NewBuffer(data), header.InternalCompression)
		for _, entry := range directory {
			if entry.RunLength > 0 {
				f(entry)
			} else {
				CollectEntries(header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length), f)
			}
		}
	}

	resolver := newResolver(deduplicate, false)
	tmpfile, err := os.CreateTemp("", "pmtiles")
	if err != nil {
		return err
	}

	bar := progressbar.Default(int64(header.TileEntriesCount))

	CollectEntries(header.RootOffset, header.RootLength, func(e EntryV3) {
		data, _ := io.ReadAll(io.NewSectionReader(file, int64(header.TileDataOffset+e.Offset), int64(e.Length)))
		if isNew, newData := resolver.AddTileIsNew(e.TileID, data, e.RunLength); isNew {
			tmpfile.Write(newData)
		}
		bar.Add(1)
	})
	file.Close()

	header.Clustered = true
	newHeader, err := finalize(logger, resolver, header, tmpfile, InputPMTiles, metadata)
	if err != nil {
		return err
	}
	fmt.Printf("total directory size %d (%f%% of original)\n", newHeader.RootLength+newHeader.LeafDirectoryLength, float64(newHeader.RootLength+newHeader.LeafDirectoryLength)/float64(header.RootLength+header.LeafDirectoryLength)*100)
	return nil
}
