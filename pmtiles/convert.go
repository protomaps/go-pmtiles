package pmtiles

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/schollz/progressbar/v3"
	"zombiezen.com/go/sqlite"
)

type offsetLen struct {
	Offset uint64
	Length uint32
}

type resolver struct {
	deduplicate    bool
	compress       bool
	Entries        []EntryV3
	Offset         uint64
	OffsetMap      map[string]offsetLen
	AddressedTiles uint64 // none of them can be empty
	compressor     *gzip.Writer
	compressTmp    *bytes.Buffer
	hashfunc       hash.Hash
}

func (r *resolver) NumContents() uint64 {
	if r.deduplicate {
		return uint64(len(r.OffsetMap))
	}
	return r.AddressedTiles
}

// must be called in increasing tile_id order, uniquely
func (r *resolver) AddTileIsNew(tileID uint64, data []byte, runLength uint32) (bool, []byte) {
	r.AddressedTiles++
	var found offsetLen
	var ok bool
	var sumString string
	if r.deduplicate {
		r.hashfunc.Reset()
		r.hashfunc.Write(data)
		var tmp []byte
		sumString = string(r.hashfunc.Sum(tmp))
		found, ok = r.OffsetMap[sumString]
	}

	if r.deduplicate && ok {
		lastEntry := r.Entries[len(r.Entries)-1]
		if tileID == lastEntry.TileID+uint64(lastEntry.RunLength) && lastEntry.Offset == found.Offset {
			// RLE
			if lastEntry.RunLength+runLength > math.MaxUint32 {
				panic("Maximum 32-bit run length exceeded")
			}
			r.Entries[len(r.Entries)-1].RunLength += runLength
		} else {
			r.Entries = append(r.Entries, EntryV3{tileID, found.Offset, found.Length, runLength})
		}

		return false, nil
	}
	var newData []byte
	if !r.compress || (len(data) >= 2 && data[0] == 31 && data[1] == 139) {
		// the tile is already compressed
		newData = data
	} else {
		r.compressTmp.Reset()
		r.compressor.Reset(r.compressTmp)
		r.compressor.Write(data)
		r.compressor.Close()
		newData = r.compressTmp.Bytes()
	}

	if r.deduplicate {
		r.OffsetMap[sumString] = offsetLen{r.Offset, uint32(len(newData))}
	}
	r.Entries = append(r.Entries, EntryV3{tileID, r.Offset, uint32(len(newData)), runLength})
	r.Offset += uint64(len(newData))
	return true, newData
}

func newResolver(deduplicate bool, compress bool) *resolver {
	b := new(bytes.Buffer)
	compressor, _ := gzip.NewWriterLevel(b, gzip.BestCompression)
	r := resolver{deduplicate, compress, make([]EntryV3, 0), 0, make(map[string]offsetLen), 0, compressor, b, fnv.New128a()}
	return &r
}

// Convert an existing archive on disk to a new PMTiles specification version 3 archive.
func Convert(logger *log.Logger, input string, output string, deduplicate bool, tmpfile *os.File) error {
	return convertMbtiles(logger, input, output, deduplicate, tmpfile)
}

func setZoomCenterDefaults(header *HeaderV3, entries []EntryV3) {
	minZ, _, _ := IDToZxy(entries[0].TileID)
	header.MinZoom = minZ
	maxZ, _, _ := IDToZxy(entries[len(entries)-1].TileID)
	header.MaxZoom = maxZ

	if header.CenterZoom == 0 && header.CenterLonE7 == 0 && header.CenterLatE7 == 0 {
		header.CenterZoom = header.MinZoom
		header.CenterLonE7 = (header.MinLonE7 + header.MaxLonE7) / 2
		header.CenterLatE7 = (header.MinLatE7 + header.MaxLatE7) / 2
	}
}

func convertMbtiles(logger *log.Logger, input string, output string, deduplicate bool, tmpfile *os.File) error {
	start := time.Now()
	conn, err := sqlite.OpenConn(input, sqlite.OpenReadOnly)
	if err != nil {
		return fmt.Errorf("Failed to create database connection, %w", err)
	}
	defer conn.Close()

	mbtilesMetadata := make([]string, 0)
	{
		stmt, _, err := conn.PrepareTransient("SELECT name, value FROM metadata")
		if err != nil {
			return fmt.Errorf("Failed to create SQL statement, %w", err)
		}
		defer stmt.Finalize()

		for {
			row, err := stmt.Step()
			if err != nil {
				return fmt.Errorf("Failed to step statement, %w", err)
			}
			if !row {
				break
			}
			mbtilesMetadata = append(mbtilesMetadata, stmt.ColumnText(0))
			mbtilesMetadata = append(mbtilesMetadata, stmt.ColumnText(1))
		}
	}

	if !mbtilesMetadataHasFormat(mbtilesMetadata) {
		logger.Println("WARNING: MBTiles metadata is missing format information. Update this with: INSERT INTO metadata (name, value) VALUES ('format', 'png')")
	}

	header, jsonMetadata, err := mbtilesToHeaderJSON(mbtilesMetadata)

	if err != nil {
		return fmt.Errorf("Failed to convert MBTiles to header JSON, %w", err)
	}

	logger.Println("Pass 1: Assembling TileID set")
	// assemble a sorted set of all TileIds
	tileset := roaring64.New()
	{
		stmt, _, err := conn.PrepareTransient("SELECT zoom_level, tile_column, tile_row FROM tiles")
		if err != nil {
			return fmt.Errorf("Failed to create statement, %w", err)
		}
		defer stmt.Finalize()

		for {
			row, err := stmt.Step()
			if err != nil {
				return fmt.Errorf("Failed to step statement, %w", err)
			}
			if !row {
				break
			}
			z := uint8(stmt.ColumnInt64(0))
			x := uint32(stmt.ColumnInt64(1))
			y := uint32(stmt.ColumnInt64(2))
			flippedY := (1 << z) - 1 - y
			id := ZxyToID(z, x, flippedY)
			tileset.Add(id)
		}
	}

	if tileset.GetCardinality() == 0 {
		return fmt.Errorf("no tiles in MBTiles archive")
	}

	logger.Println("Pass 2: writing tiles")
	resolve := newResolver(deduplicate, header.TileType == Mvt)
	{
		bar := progressbar.Default(int64(tileset.GetCardinality()))
		i := tileset.Iterator()
		stmt := conn.Prep("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?")

		var rawTileTmp bytes.Buffer

		for i.HasNext() {
			id := i.Next()
			z, x, y := IDToZxy(id)
			flippedY := (1 << z) - 1 - y

			stmt.BindInt64(1, int64(z))
			stmt.BindInt64(2, int64(x))
			stmt.BindInt64(3, int64(flippedY))

			hasRow, err := stmt.Step()
			if err != nil {
				return fmt.Errorf("Failed to step statement, %w", err)
			}
			if !hasRow {
				return fmt.Errorf("Missing row")
			}

			reader := stmt.ColumnReader(0)
			rawTileTmp.Reset()
			rawTileTmp.ReadFrom(reader)
			data := rawTileTmp.Bytes()

			if len(data) > 0 {
				if isNew, newData := resolve.AddTileIsNew(id, data, 1); isNew {
					_, err := tmpfile.Write(newData)
					if err != nil {
						return fmt.Errorf("Failed to write to tempfile: %s", err)
					}
				}
			}

			stmt.ClearBindings()
			stmt.Reset()
			bar.Add(1)
		}
	}
	_, err = finalize(logger, resolve, header, tmpfile, output, jsonMetadata)
	if err != nil {
		return err
	}
	logger.Println("Finished in ", time.Since(start))
	return nil
}

func finalize(logger *log.Logger, resolve *resolver, header HeaderV3, tmpfile *os.File, output string, jsonMetadata map[string]interface{}) (HeaderV3, error) {
	logger.Println("# of addressed tiles: ", resolve.AddressedTiles)
	logger.Println("# of tile entries (after RLE): ", len(resolve.Entries))
	logger.Println("# of tile contents: ", resolve.NumContents())

	header.AddressedTilesCount = resolve.AddressedTiles
	header.TileEntriesCount = uint64(len(resolve.Entries))
	header.TileContentsCount = resolve.NumContents()

	// assemble the final file
	outfile, err := os.Create(output)
	if err != nil {
		return header, fmt.Errorf("Failed to create %s, %w", output, err)
	}
	defer outfile.Close()

	rootBytes, leavesBytes, numLeaves := optimizeDirectories(resolve.Entries, 16384-HeaderV3LenBytes, Gzip)

	if numLeaves > 0 {
		logger.Println("Root dir bytes: ", len(rootBytes))
		logger.Println("Leaves dir bytes: ", len(leavesBytes))
		logger.Println("Num leaf dirs: ", numLeaves)
		logger.Println("Total dir bytes: ", len(rootBytes)+len(leavesBytes))
		logger.Println("Average leaf dir bytes: ", len(leavesBytes)/numLeaves)
		logger.Printf("Average bytes per addressed tile: %.2f\n", float64(len(rootBytes)+len(leavesBytes))/float64(resolve.AddressedTiles))
	} else {
		logger.Println("Total dir bytes: ", len(rootBytes))
		logger.Printf("Average bytes per addressed tile: %.2f\n", float64(len(rootBytes))/float64(resolve.AddressedTiles))
	}

	metadataBytes, err := SerializeMetadata(jsonMetadata, Gzip)

	if err != nil {
		return header, fmt.Errorf("Failed to marshal metadata, %w", err)
	}

	setZoomCenterDefaults(&header, resolve.Entries)

	header.Clustered = true
	header.InternalCompression = Gzip
	if header.TileType == Mvt {
		header.TileCompression = Gzip
	}

	header.RootOffset = HeaderV3LenBytes
	header.RootLength = uint64(len(rootBytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.MetadataLength = uint64(len(metadataBytes))
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(leavesBytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength
	header.TileDataLength = resolve.Offset

	headerBytes := SerializeHeader(header)

	_, err = outfile.Write(headerBytes)
	if err != nil {
		return header, fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = outfile.Write(rootBytes)
	if err != nil {
		return header, fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = outfile.Write(metadataBytes)
	if err != nil {
		return header, fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = outfile.Write(leavesBytes)
	if err != nil {
		return header, fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = tmpfile.Seek(0, 0)
	if err != nil {
		return header, fmt.Errorf("Failed to seek to start of tempfile, %w", err)
	}
	_, err = io.Copy(outfile, tmpfile)
	if err != nil {
		return header, fmt.Errorf("Failed to copy data to outfile, %w", err)
	}

	return header, nil
}

func parseBounds(bounds string) (int32, int32, int32, int32, error) {
	parts := strings.Split(bounds, ",")
	E7 := 10000000.0
	minLon, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	minLat, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	maxLon, err := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	maxLat, err := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return int32(minLon * E7), int32(minLat * E7), int32(maxLon * E7), int32(maxLat * E7), nil
}

func parseCenter(center string) (int32, int32, uint8, error) {
	parts := strings.Split(center, ",")
	E7 := 10000000.0
	centerLon, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, 0, 0, err
	}
	centerLat, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, 0, err
	}
	centerZoom, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 8)
	if err != nil {
		return 0, 0, 0, err
	}
	return int32(centerLon * E7), int32(centerLat * E7), uint8(centerZoom), nil
}

func mbtilesMetadataHasFormat(mbtilesMetadata []string) bool {
	for i := 0; i < len(mbtilesMetadata); i += 2 {
		if mbtilesMetadata[i] == "format" {
			return true
		}
	}
	return false
}

func mbtilesToHeaderJSON(mbtilesMetadata []string) (HeaderV3, map[string]interface{}, error) {
	header := HeaderV3{}
	jsonResult := make(map[string]interface{})
	boundsSet := false
	for i := 0; i < len(mbtilesMetadata); i += 2 {
		value := mbtilesMetadata[i+1]
		switch key := mbtilesMetadata[i]; key {
		case "format":
			switch value {
			case "pbf":
				header.TileType = Mvt
			case "png":
				header.TileType = Png
				header.TileCompression = NoCompression
			case "jpg":
				header.TileType = Jpeg
				header.TileCompression = NoCompression
			case "webp":
				header.TileType = Webp
				header.TileCompression = NoCompression
			case "avif":
				header.TileType = Avif
				header.TileCompression = NoCompression
			}
			jsonResult["format"] = value
		case "bounds":
			minLon, minLat, maxLon, maxLat, err := parseBounds(value)
			if err != nil {
				return header, jsonResult, err
			}

			if minLon >= maxLon || minLat >= maxLat {
				return header, jsonResult, fmt.Errorf("zero-area bounds in mbtiles metadata")
			}
			header.MinLonE7 = minLon
			header.MinLatE7 = minLat
			header.MaxLonE7 = maxLon
			header.MaxLatE7 = maxLat
			boundsSet = true
		case "center":
			centerLon, centerLat, centerZoom, err := parseCenter(value)
			if err != nil {
				return header, jsonResult, err
			}
			header.CenterLonE7 = centerLon
			header.CenterLatE7 = centerLat
			header.CenterZoom = centerZoom
		case "json":
			var mbtilesJSON map[string]interface{}
			json.Unmarshal([]byte(value), &mbtilesJSON)
			for k, v := range mbtilesJSON {
				jsonResult[k] = v
			}
		case "compression":
			switch value {
			case "gzip":
				if header.TileType == Mvt {
					header.TileCompression = Gzip
				} else {
					header.TileCompression = NoCompression
				}
			}
			jsonResult["compression"] = value
		// name, attribution, description, type, version
		default:
			jsonResult[key] = value
		}
	}

	E7 := 10000000.0
	if !boundsSet {
		header.MinLonE7 = int32(-180 * E7)
		header.MinLatE7 = int32(-85 * E7)
		header.MaxLonE7 = int32(180 * E7)
		header.MaxLatE7 = int32(85 * E7)
	}

	return header, jsonResult, nil
}
