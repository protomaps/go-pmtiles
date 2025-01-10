package pmtiles

import (
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"io"
	"log"
	"math"
	"os"
	"time"
)

// Verify that an archive's header statistics are correct,
// and that tiles are propertly ordered if clustered=true.
func Verify(_ *log.Logger, file string) error {
	start := time.Now()
	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey("", "", file)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, 16384)

	if err != nil {
		return fmt.Errorf("failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read %s, %w", key, err)
	}
	r.Close()

	header, err := DeserializeHeader(b[0:HeaderV3LenBytes])

	if err != nil {
		return fmt.Errorf("failed to read %s, %w", key, err)
	}

	if header.RootOffset == 0 {
		return fmt.Errorf("Root directory offset=%v must not be 0", header.RootOffset)
	}

	if header.MetadataOffset == 0 {
		return fmt.Errorf("Metadata offset=%v must not be 0", header.MetadataOffset)
	}

	if header.LeafDirectoryOffset == 0 {
		return fmt.Errorf("Leaf directories offset=%v must not be 0", header.LeafDirectoryOffset)
	}

	if header.TileDataOffset == 0 {
		return fmt.Errorf("Tile data offset=%v must not be 0", header.TileDataOffset)
	}

	fileInfo, _ := os.Stat(file)

	if header.RootLength > uint64(fileInfo.Size()) {
		return fmt.Errorf("Root directory offset=%v length=%v out of bounds", header.RootOffset, header.RootLength)
	}

	if header.MetadataLength > uint64(fileInfo.Size()) {
		return fmt.Errorf("Metadata offset=%v length=%v out of bounds", header.MetadataOffset, header.MetadataLength)
	}

	if header.LeafDirectoryLength > uint64(fileInfo.Size()) {
		return fmt.Errorf("Leaf directories offset=%v length=%v out of bounds", header.LeafDirectoryOffset, header.LeafDirectoryLength)
	}

	if header.TileDataLength > uint64(fileInfo.Size()) {
		return fmt.Errorf("Tile data offset=%v length=%v out of bounds", header.TileDataOffset, header.TileDataLength)
	}

	lengthFromHeader := int64(HeaderV3LenBytes + header.RootLength + header.MetadataLength + header.LeafDirectoryLength + header.TileDataLength)
	lengthFromHeaderWithPadding := int64(16384 + header.MetadataLength + header.LeafDirectoryLength + header.TileDataLength)

	if !(fileInfo.Size() == lengthFromHeader || fileInfo.Size() == lengthFromHeaderWithPadding) {
		return fmt.Errorf("total length of archive %v does not match header %v or %v (padded)", fileInfo.Size(), lengthFromHeader, lengthFromHeaderWithPadding)
	}

	var minTileID uint64
	var maxTileID uint64
	minTileID = math.MaxUint64
	maxTileID = 0

	addressedTiles := 0
	tileEntries := 0
	offsets := roaring64.New()
	var currentOffset uint64

	err = IterateEntries(header,
		func(offset uint64, length uint64) ([]byte, error) {
			reader, err := bucket.NewRangeReader(ctx, key, int64(offset), int64(length))
			if err != nil {
				return nil, err
			}
			defer reader.Close()
			return io.ReadAll(reader)
		},
		func(e EntryV3) {
			offsets.Add(e.Offset)
			addressedTiles += int(e.RunLength)
			tileEntries++

			if e.TileID < minTileID {
				minTileID = e.TileID
			}
			if e.TileID > maxTileID {
				maxTileID = e.TileID
			}

			if e.Offset+uint64(e.Length) > header.TileDataLength {
				fmt.Printf("Invalid: %v outside of tile data section", e)
			}

			if header.Clustered {
				if !offsets.Contains(e.Offset) {
					if e.Offset != currentOffset {
						fmt.Printf("Invalid: out-of-order entry %v in clustered archive", e)
					}
					currentOffset += uint64(e.Length)
				}
			}
		})

	if err != nil {
		return err
	}

	if uint64(addressedTiles) != header.AddressedTilesCount {
		return fmt.Errorf("invalid: header AddressedTilesCount=%v but %v tiles addressed", header.AddressedTilesCount, addressedTiles)
	}

	if uint64(tileEntries) != header.TileEntriesCount {
		return fmt.Errorf("invalid: header TileEntriesCount=%v but %v tile entries", header.TileEntriesCount, tileEntries)
	}

	if offsets.GetCardinality() != header.TileContentsCount {
		return fmt.Errorf("invalid: header TileContentsCount=%v but %v tile contents", header.TileContentsCount, offsets.GetCardinality())
	}

	if z, _, _ := IDToZxy(minTileID); z != header.MinZoom {
		return fmt.Errorf("invalid: header MinZoom=%v does not match min tile z %v", header.MinZoom, z)
	}

	if z, _, _ := IDToZxy(maxTileID); z != header.MaxZoom {
		return fmt.Errorf("invalid: header MaxZoom=%v does not match max tile z %v", header.MaxZoom, z)
	}

	if !(header.CenterZoom >= header.MinZoom && header.CenterZoom <= header.MaxZoom) {
		return fmt.Errorf("invalid: header CenterZoom=%v not within MinZoom/MaxZoom", header.CenterZoom)
	}

	if header.MinLonE7 >= header.MaxLonE7 || header.MinLatE7 >= header.MaxLatE7 {
		return fmt.Errorf("Invalid: bounds has area <= 0: clients may not display tiles correctly")
	}

	fmt.Printf("Completed verify in %v.\n", time.Since(start))
	return nil
}
