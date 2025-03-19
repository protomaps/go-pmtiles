package main

import (
	"encoding/base64"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"os"
)

func main() {
	PINK := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z/D/PwAHAwL/qGeMxAAAAABJRU5ErkJggg=="
	CYAN := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+P//PwAGBAL/VJiKjgAAAABJRU5ErkJggg=="

	pink, _ := base64.StdEncoding.DecodeString(PINK)
	cyan, _ := base64.StdEncoding.DecodeString(CYAN)

	outfile, _ := os.Create("output.pmtiles")

	var h pmtiles.HeaderV3
	var entries []pmtiles.EntryV3
	entries = append(entries, pmtiles.EntryV3{1, uint64(len(cyan)), uint32(len(pink)), 1})
	entries = append(entries, pmtiles.EntryV3{2, 0, uint32(len(cyan)), 1})
	dir := pmtiles.SerializeEntries(entries, pmtiles.NoCompression)
	h.SpecVersion = 3
	h.RootOffset = 127
	h.RootLength = uint64(len(dir))
	h.MetadataOffset = uint64(127 + len(dir))
	h.MetadataLength = 2
	h.LeafDirectoryOffset = h.MetadataOffset + h.MetadataLength
	h.LeafDirectoryLength = 0
	h.TileDataOffset = h.LeafDirectoryOffset
	h.TileDataLength = uint64(len(pink) + len(cyan))
	h.AddressedTilesCount = 2
	h.TileEntriesCount = 2
	h.TileContentsCount = 2
	h.InternalCompression = 1
	h.TileCompression = 1
	h.TileType = 2
	h.MinZoom = 1
	h.CenterZoom = 1
	h.MaxZoom = 1
	h.MinLatE7 = -85 * 10000000
	h.MaxLatE7 = 85 * 10000000
	h.MinLonE7 = -180 * 10000000
	h.MaxLonE7 = 180 * 10000000

	outfile.Write(pmtiles.SerializeHeader(h))
	outfile.Write(dir)
	outfile.Write([]byte("{}"))
	outfile.Write(cyan)
	outfile.Write(pink)
}
