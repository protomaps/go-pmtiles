package pmtiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	// "github.com/dustin/go-humanize"
	"io"
	"log"
	"os"
)

func Show(logger *log.Logger, bucketURL string, key string, show_tile bool, z int, x int, y int) error {
	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey(bucketURL, "", key)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, 16384)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Failed to read %s, %w", key, err)
	}
	r.Close()

	header, err := deserialize_header(b[0:HEADERV3_LEN_BYTES])
	if err != nil {
		// check to see if it's a V2 file
		if string(b[0:2]) == "PM" {
			spec_version := b[2]
			return fmt.Errorf("PMTiles version %d detected; please use 'pmtiles convert' to upgrade to version 3.", spec_version)
		}

		return fmt.Errorf("Failed to read %s, %w", key, err)
	}

	if !show_tile {
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
		case Avif:
			tile_type = "Raster AVIF"
		default:
			tile_type = "Unknown"
		}
		fmt.Printf("pmtiles spec version: %d\n", header.SpecVersion)
		// fmt.Printf("total size: %s\n", humanize.Bytes(uint64(r.Size())))
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
		fmt.Printf("internal compression: %d\n", header.InternalCompression)
		fmt.Printf("tile compression: %d\n", header.TileCompression)

		metadata_reader, err := bucket.NewRangeReader(ctx, key, int64(header.MetadataOffset), int64(header.MetadataLength))
		if err != nil {
			return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
		}

		var metadata_bytes []byte
		if header.InternalCompression == Gzip {
			r, _ := gzip.NewReader(metadata_reader)
			metadata_bytes, err = io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("Failed to read %s, %w", key, err)
			}
		} else {
			metadata_bytes, err = io.ReadAll(metadata_reader)
			if err != nil {
				return fmt.Errorf("Failed to read %s, %w", key, err)
			}
		}
		metadata_reader.Close()

		var metadata_map map[string]interface{}
		json.Unmarshal(metadata_bytes, &metadata_map)
		for k, v := range metadata_map {
			switch v := v.(type) {
			case string:
				fmt.Println(k, v)
			default:
				fmt.Println(k, "<object...>")
			}
		}

	} else {
		// write the tile to stdout

		tile_id := ZxyToId(uint8(z), uint32(x), uint32(y))

		dir_offset := header.RootOffset
		dir_length := header.RootLength

		for depth := 0; depth <= 3; depth++ {
			r, err := bucket.NewRangeReader(ctx, key, int64(dir_offset), int64(dir_length))
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
					tile_r, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+entry.Offset), int64(entry.Length))
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
