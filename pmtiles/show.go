package pmtiles

import (
	"context"
	"fmt"
	"gocloud.dev/blob"
	"io"
	"log"
)

func Show(logger *log.Logger, args []string) {
	// if a local file path, construct a bucket/key

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "file://.")
	if err != nil {
		logger.Fatal(err)
	}
	defer bucket.Close()

	// Open the key "foo.txt" for reading at offset 1024 and read up to 4096 bytes.
	r, err := bucket.NewRangeReader(ctx, "out5.pmtiles", 0, 16384, nil)
	if err != nil {
		logger.Fatal(err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		logger.Fatal(err)
	}
	r.Close()

	header := deserialize_header(b[0:HEADERV3_LEN_BYTES])
	var tile_type string
	switch header.TileType {
	case Mvt:
		tile_type = "Vector Protobuf (MVT)"
	case Png:
		tile_type = "Raster PNG"
	case Jpeg:
		tile_type = "Raster Jpeg"
	case Webp:
		tile_type = "Raster WebP"
	default:
		tile_type = "Unknown"
	}
	fmt.Printf("tile type: %s\n", tile_type)
	fmt.Printf("bounds: %f,%f %f,%f\n", float64(header.MinLonE7)/10000000, float64(header.MinLatE7)/10000000, float64(header.MaxLonE7)/10000000, float64(header.MaxLatE7)/10000000)
	fmt.Printf("min zoom: %d\n", header.MinZoom)
	fmt.Printf("max zoom: %d\n", header.MaxZoom)
	fmt.Printf("center: %f,%f\n", float64(header.CenterLonE7)/10000000, float64(header.CenterLatE7)/10000000)
	fmt.Printf("center zoom: %d\n", header.CenterZoom)
	fmt.Printf("addressed tiles count: %d\n", header.AddressedTilesCount)
	fmt.Printf("tile entries count: %d\n", header.TileEntriesCount)
	fmt.Printf("tile contents count: %d\n", header.TileContentsCount)
	fmt.Printf("clustered: %t\n", header.Clustered)

	// entries := deserialize_entries(bytes.NewBuffer(b[header.RootOffset:header.RootOffset+header.RootLength]))
	// for _, entry := range entries {
	// 	fmt.Println(entry)
	// }
}
