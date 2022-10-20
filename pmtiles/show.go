package pmtiles

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/dustin/go-humanize"
	"gocloud.dev/blob"
	"io"
	"log"
	"os"
	"strconv"
)

func Show(logger *log.Logger, args []string) error {
	cmd := flag.NewFlagSet("show", flag.ExitOnError)
	cmd.Parse(args)
	bucketURL := cmd.Arg(0)
	file := cmd.Arg(1)
	arg_z := cmd.Arg(2)
	arg_x := cmd.Arg(3)
	arg_y := cmd.Arg(4)

	if bucketURL == "" || file == "" {
		return fmt.Errorf("USAGE: show BUCKET_URL KEY")
	}

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, file, 0, 16384, nil)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", file, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Failed to read %s, %w", file, err)
	}
	r.Close()

	header := deserialize_header(b[0:HEADERV3_LEN_BYTES])

	if arg_z == "" {
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
		fmt.Printf("total size: %s\n", humanize.Bytes(uint64(r.Size())))
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
	} else {
		// write the tile to stdout

		z, _ := strconv.ParseUint(arg_z, 10, 8)
		x, _ := strconv.ParseUint(arg_x, 10, 32)
		y, _ := strconv.ParseUint(arg_y, 10, 32)
		tile_id := ZxyToId(uint8(z), uint32(x), uint32(y))

		dir_offset := header.RootOffset
		dir_length := header.RootLength

		for depth := 0; depth <= 3; depth++ {
			r, err := bucket.NewRangeReader(ctx, file, int64(dir_offset), int64(dir_length), nil)
			if err != nil {
				return fmt.Errorf("Network error")
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("I/O Error")
			}
			directory := deserialize_entries(bytes.NewBuffer(b))
			entry, ok := find_tile(directory, tile_id)
			if ok {
				if entry.RunLength > 0 {
					tile_r, err := bucket.NewRangeReader(ctx, file, int64(header.TileDataOffset+entry.Offset), int64(entry.Length), nil)
					if err != nil {
						return fmt.Errorf("Network error")
					}
					defer tile_r.Close()
					tile_b, err := io.ReadAll(tile_r)
					if err != nil {
						return fmt.Errorf("I/O Error")
					}
					os.Stdout.Write(tile_b)
					break
				} else {
					dir_offset = header.LeafDirectoryOffset + entry.Offset
					dir_length = uint64(entry.Length)
				}
			} else {
				fmt.Println("Tile not found in archive.")
				return nil
			}
		}
	}
	return nil
}
