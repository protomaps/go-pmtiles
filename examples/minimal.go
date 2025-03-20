package main

import (
	"encoding/base64"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"os"
)

// A program that creates the smallest meaningful PMTiles archive,
// consisting of a purple square at tile 0,0,0 (the entire earth).
// Uses only two library functions, SerializeHeader and SerializeEntries.
func main() {
	outfile, _ := os.Create("minimal.pmtiles")
	defer outfile.Close()

	// A solid purple PNG with 50% opacity.
	png, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mM0NLxTDwADmAG/Djok1gAAAABJRU5ErkJggg==")


	// Create an entry with TileID=0 (z=0, x=0, y=0), Offset=0, Length=len(png), and RunLength=1.
	entries := []pmtiles.EntryV3{{0, 0, uint32(len(png)), 1}}

	// Create the bytes of the root directory.
	dir := pmtiles.SerializeEntries(entries, pmtiles.NoCompression)

	// the JSON metadata is the empty object.
	metadata := "{}"

	// here we set the data of the header (the first 127 bytes)
	var h pmtiles.HeaderV3
	h.SpecVersion = 3

	// the root directory follows the header.
	h.RootOffset = pmtiles.HeaderV3LenBytes
	h.RootLength = uint64(len(dir))

	// the JSON metadata follows the root directory.
	h.MetadataOffset = h.RootOffset + uint64(len(dir))
	h.MetadataLength = uint64(len(metadata))

	// there are no leaves, but set the offset to the right place and length=0.
	h.LeafDirectoryOffset = h.MetadataOffset + h.MetadataLength
	h.LeafDirectoryLength = 0

	// the tile data follows the JSON metadata.
	h.TileDataOffset = h.LeafDirectoryOffset
	h.TileDataLength = uint64(len(png))

	// set statistics
	h.AddressedTilesCount = 1
	h.TileEntriesCount = 1
	h.TileContentsCount = 1

	// since we store a PNG, the tile data should not be interpreted as compressed.
	h.InternalCompression = pmtiles.NoCompression
	h.TileCompression = pmtiles.NoCompression
	h.TileType = pmtiles.Png

	// set the zoom and geographic bounds.
	h.MinZoom = 0
	h.CenterZoom = 0
	h.MaxZoom = 0
	h.MinLatE7 = -85 * 10000000
	h.MaxLatE7 = 85 * 10000000
	h.MinLonE7 = -180 * 10000000
	h.MaxLonE7 = 180 * 10000000

	outfile.Write(pmtiles.SerializeHeader(h))

	// write the directory, JSON metadata and tile data.
	outfile.Write(dir)
	outfile.Write([]byte(metadata))
	outfile.Write(png)
}
