package pmtiles

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"sort"
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
func (r *resolver) AddTileIsNew(tileID uint64, data []byte) (bool, []byte) {
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
			if lastEntry.RunLength+1 > math.MaxUint32 {
				panic("Maximum 32-bit run length exceeded")
			}
			r.Entries[len(r.Entries)-1].RunLength++
		} else {
			r.Entries = append(r.Entries, EntryV3{tileID, found.Offset, found.Length, 1})
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
	r.Entries = append(r.Entries, EntryV3{tileID, r.Offset, uint32(len(newData)), 1})
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
	if strings.HasSuffix(input, ".pmtiles") {
		return convertPmtilesV2(logger, input, output, deduplicate, tmpfile)
	}
	return convertMbtiles(logger, input, output, deduplicate, tmpfile)
}

func addDirectoryV2Entries(dir directoryV2, entries *[]EntryV3, f *os.File) {
	for zxy, rng := range dir.Entries {
		tileID := ZxyToID(zxy.Z, zxy.X, zxy.Y)
		*entries = append(*entries, EntryV3{tileID, rng.Offset, uint32(rng.Length), 1})
	}

	var unique = map[uint64]uint32{}

	// uniqify the offset/length pairs
	for _, rng := range dir.Leaves {
		unique[rng.Offset] = uint32(rng.Length)
	}

	for offset, length := range unique {
		f.Seek(int64(offset), 0)
		leafBytes := make([]byte, length)
		f.Read(leafBytes)
		leafDir := parseDirectoryV2(leafBytes)
		addDirectoryV2Entries(leafDir, entries, f)
	}
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

func convertPmtilesV2(logger *log.Logger, input string, output string, deduplicate bool, tmpfile *os.File) error {
	start := time.Now()
	f, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("Failed to open file: %w", err)
	}
	defer f.Close()
	buffer := make([]byte, 512000)
	io.ReadFull(f, buffer)
	if string(buffer[0:7]) == "PMTiles" && buffer[7] == 3 {
		return fmt.Errorf("archive is already the latest PMTiles version (3)")
	}

	v2JsonBytes, dir := parseHeaderV2(bytes.NewReader(buffer))

	var v2metadata map[string]interface{}
	json.Unmarshal(v2JsonBytes, &v2metadata)

	// get the first 4 bytes at offset 512000 to attempt tile type detection

	first4 := make([]byte, 4)
	f.Seek(512000, 0)
	n, err := f.Read(first4)
	if n != 4 || err != nil {
		return fmt.Errorf("Failed to read first 4, %w", err)
	}

	header, jsonMetadata, err := v2ToHeaderJSON(v2metadata, first4)

	if err != nil {
		return fmt.Errorf("Failed to convert v2 to header JSON, %w", err)
	}

	entries := make([]EntryV3, 0)
	addDirectoryV2Entries(dir, &entries, f)

	// sort
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TileID < entries[j].TileID
	})

	// re-use resolve, because even if archives are de-duplicated we may need to recompress.
	resolve := newResolver(deduplicate, header.TileType == Mvt)

	bar := progressbar.Default(int64(len(entries)))
	for _, entry := range entries {
		if entry.Length == 0 {
			continue
		}
		_, err := f.Seek(int64(entry.Offset), 0)
		if err != nil {
			return fmt.Errorf("Failed to seek at offset %d, %w", entry.Offset, err)
		}
		buf := make([]byte, entry.Length)
		_, err = f.Read(buf)
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("Failed to read buffer, %w", err)
			}
		}
		// TODO: enforce sorted order
		if isNew, newData := resolve.AddTileIsNew(entry.TileID, buf); isNew {
			_, err = tmpfile.Write(newData)
			if err != nil {
				return fmt.Errorf("Failed to write to tempfile, %w", err)
			}
		}
		bar.Add(1)
	}

	err = finalize(logger, resolve, header, tmpfile, output, jsonMetadata)
	if err != nil {
		return err
	}

	logger.Println("Finished in ", time.Since(start))
	return nil
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
				if isNew, newData := resolve.AddTileIsNew(id, data); isNew {
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
	err = finalize(logger, resolve, header, tmpfile, output, jsonMetadata)
	if err != nil {
		return err
	}
	logger.Println("Finished in ", time.Since(start))
	return nil
}

func finalize(logger *log.Logger, resolve *resolver, header HeaderV3, tmpfile *os.File, output string, jsonMetadata map[string]interface{}) error {
	logger.Println("# of addressed tiles: ", resolve.AddressedTiles)
	logger.Println("# of tile entries (after RLE): ", len(resolve.Entries))
	logger.Println("# of tile contents: ", resolve.NumContents())

	header.AddressedTilesCount = resolve.AddressedTiles
	header.TileEntriesCount = uint64(len(resolve.Entries))
	header.TileContentsCount = resolve.NumContents()

	// assemble the final file
	outfile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("Failed to create %s, %w", output, err)
	}

	rootBytes, leavesBytes, numLeaves := optimizeDirectories(resolve.Entries, 16384-HeaderV3LenBytes)

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

	var metadataBytes []byte
	{
		metadataBytesUncompressed, err := json.Marshal(jsonMetadata)
		if err != nil {
			return fmt.Errorf("Failed to marshal metadata, %w", err)
		}
		var b bytes.Buffer
		w, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
		w.Write(metadataBytesUncompressed)
		w.Close()
		metadataBytes = b.Bytes()
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

	headerBytes := serializeHeader(header)

	_, err = outfile.Write(headerBytes)
	if err != nil {
		return fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = outfile.Write(rootBytes)
	if err != nil {
		return fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = outfile.Write(metadataBytes)
	if err != nil {
		return fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = outfile.Write(leavesBytes)
	if err != nil {
		return fmt.Errorf("Failed to write header to outfile, %w", err)
	}
	_, err = tmpfile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("Failed to seek to start of tempfile, %w", err)
	}
	_, err = io.Copy(outfile, tmpfile)
	if err != nil {
		return fmt.Errorf("Failed to copy data to outfile, %w", err)
	}

	return nil
}

func v2ToHeaderJSON(v2JsonMetadata map[string]interface{}, first4 []byte) (HeaderV3, map[string]interface{}, error) {
	header := HeaderV3{}

	if val, ok := v2JsonMetadata["bounds"]; ok {
		minLon, minLat, maxLon, maxLat, err := parseBounds(val.(string))
		if err != nil {
			return header, v2JsonMetadata, err
		}
		header.MinLonE7 = minLon
		header.MinLatE7 = minLat
		header.MaxLonE7 = maxLon
		header.MaxLatE7 = maxLat
		delete(v2JsonMetadata, "bounds")
	} else {
		return header, v2JsonMetadata, errors.New("archive is missing bounds")
	}

	if val, ok := v2JsonMetadata["center"]; ok {
		centerLon, centerLat, centerZoom, err := parseCenter(val.(string))
		if err != nil {
			return header, v2JsonMetadata, err
		}
		header.CenterLonE7 = centerLon
		header.CenterLatE7 = centerLat
		header.CenterZoom = centerZoom
		delete(v2JsonMetadata, "center")
	}

	if val, ok := v2JsonMetadata["compression"]; ok {
		switch val.(string) {
		case "gzip":
			header.TileCompression = Gzip
		default:
			return header, v2JsonMetadata, errors.New("Unknown compression type")
		}
	} else {
		if first4[0] == 0x1f && first4[1] == 0x8b {
			header.TileCompression = Gzip
		}
	}

	if val, ok := v2JsonMetadata["format"]; ok {
		switch val.(string) {
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
		default:
			return header, v2JsonMetadata, errors.New("Unknown tile type")
		}
	} else {
		if first4[0] == 0x89 && first4[1] == 0x50 && first4[2] == 0x4e && first4[3] == 0x47 {
			header.TileType = Png
			header.TileCompression = NoCompression
		} else if first4[0] == 0xff && first4[1] == 0xd8 && first4[2] == 0xff && first4[3] == 0xe0 {
			header.TileType = Jpeg
			header.TileCompression = NoCompression
		} else {
			// assume it is a vector tile
			header.TileType = Mvt
		}
	}

	// deserialize embedded JSON and lift keys to top-level
	// to avoid "json-in-json"
	if val, ok := v2JsonMetadata["json"]; ok {
		stringVal := val.(string)
		var inside map[string]interface{}
		json.Unmarshal([]byte(stringVal), &inside)
		for k, v := range inside {
			v2JsonMetadata[k] = v
		}
		delete(v2JsonMetadata, "json")
	}

	return header, v2JsonMetadata, nil
}

func parseBounds(bounds string) (int32, int32, int32, int32, error) {
	parts := strings.Split(bounds, ",")
	E7 := 10000000.0
	minLon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	minLat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	maxLon, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	maxLat, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return int32(minLon * E7), int32(minLat * E7), int32(maxLon * E7), int32(maxLat * E7), nil
}

func parseCenter(center string) (int32, int32, uint8, error) {
	parts := strings.Split(center, ",")
	E7 := 10000000.0
	centerLon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	centerLat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	centerZoom, err := strconv.ParseInt(parts[2], 10, 8)
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
