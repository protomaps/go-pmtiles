package pmtiles

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Edit parts of the header or metadata.
// works in-place if only the header is modified.
func Edit(_ *log.Logger, inputArchive string, newHeaderJSONFile string, newMetadataFile string) error {
	if newHeaderJSONFile == "" && newMetadataFile == "" {
		return fmt.Errorf("must supply --header-json and/or --metadata to edit")
	}

	file, err := os.OpenFile(inputArchive, os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		return err
	}

	buf := make([]byte, 127)
	_, err = file.Read(buf)
	if err != nil {
		return err
	}
	oldHeader, err := DeserializeHeader(buf)
	if err != nil {
		return err
	}

	newHeader := oldHeader

	if newHeaderJSONFile != "" {
		newHeaderData := HeaderJson{}
		data, err := ioutil.ReadFile(newHeaderJSONFile)
		if err != nil {
			return err
		}
		err = json.Unmarshal(data, &newHeaderData)
		if err != nil {
			return err
		}

		if len(newHeaderData.Bounds) != 4 {
			return fmt.Errorf("header len(bounds) must == 4")
		}
		if len(newHeaderData.Center) != 3 {
			return fmt.Errorf("header len(center) must == 3")
		}

		newHeader.TileType = stringToTileType(newHeaderData.TileType)
		newHeader.TileCompression = stringToCompression(newHeaderData.TileCompression)
		newHeader.MinZoom = uint8(newHeaderData.MinZoom)
		newHeader.MaxZoom = uint8(newHeaderData.MaxZoom)
		newHeader.MinLonE7 = int32(newHeaderData.Bounds[0] * 10000000)
		newHeader.MinLatE7 = int32(newHeaderData.Bounds[1] * 10000000)
		newHeader.MaxLonE7 = int32(newHeaderData.Bounds[2] * 10000000)
		newHeader.MaxLatE7 = int32(newHeaderData.Bounds[3] * 10000000)
		newHeader.CenterLonE7 = int32(newHeaderData.Center[0] * 10000000)
		newHeader.CenterLatE7 = int32(newHeaderData.Center[1] * 10000000)
		newHeader.CenterZoom = uint8(newHeaderData.Center[2])
	}

	if newMetadataFile == "" {
		buf = SerializeHeader(newHeader)
		_, err = file.WriteAt(buf, 0)
		if err != nil {
			return err
		}
		file.Close()
		return nil
	}

	metadataReader, err := os.Open(newMetadataFile)
	if err != nil {
		return err
	}
	defer metadataReader.Close()

	parsedMetadata, err := DeserializeMetadata(metadataReader, NoCompression)
	if err != nil {
		return err
	}

	metadataBytes, err := SerializeMetadata(parsedMetadata, oldHeader.InternalCompression)
	if err != nil {
		return err
	}

	tempFilePath := inputArchive + ".tmp"

	if _, err = os.Stat(tempFilePath); err == nil {
		return fmt.Errorf("A file with the same name already exists")
	}

	outfile, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}
	defer outfile.Close()

	newHeader.MetadataOffset = newHeader.RootOffset + newHeader.RootLength
	newHeader.MetadataLength = uint64(len(metadataBytes))
	newHeader.LeafDirectoryOffset = newHeader.MetadataOffset + newHeader.MetadataLength
	newHeader.TileDataOffset = newHeader.LeafDirectoryOffset + newHeader.LeafDirectoryLength

	bar := progressbar.DefaultBytes(
		int64(HeaderV3LenBytes+newHeader.RootLength+uint64(len(metadataBytes))+newHeader.LeafDirectoryLength+newHeader.TileDataLength),
		"writing file",
	)

	buf = SerializeHeader(newHeader)
	io.Copy(io.MultiWriter(outfile, bar), bytes.NewReader(buf))

	rootSection := io.NewSectionReader(file, int64(oldHeader.RootOffset), int64(oldHeader.RootLength))
	if _, err := io.Copy(io.MultiWriter(outfile, bar), rootSection); err != nil {
		return err
	}

	if _, err := io.Copy(io.MultiWriter(outfile, bar), bytes.NewReader(metadataBytes)); err != nil {
		return err
	}

	leafSection := io.NewSectionReader(file, int64(oldHeader.LeafDirectoryOffset), int64(oldHeader.LeafDirectoryLength))
	if _, err := io.Copy(io.MultiWriter(outfile, bar), leafSection); err != nil {
		return err
	}

	tileSection := io.NewSectionReader(file, int64(oldHeader.TileDataOffset), int64(oldHeader.TileDataLength))
	if _, err := io.Copy(io.MultiWriter(outfile, bar), tileSection); err != nil {
		return err
	}

	// explicitly close in order to rename
	file.Close()
	outfile.Close()
	if err := os.Rename(tempFilePath, inputArchive); err != nil {
		return err
	}
	return nil
}
