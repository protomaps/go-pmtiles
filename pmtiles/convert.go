package pmtiles

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/schollz/progressbar/v3"
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"zombiezen.com/go/sqlite"
)

type OffsetLen struct {
	Offset uint64
	Length uint32
}

type Resolver struct {
	deduplicate    bool
	compress       bool
	Entries        []EntryV3
	Offset         uint64
	OffsetMap      map[string]OffsetLen
	AddressedTiles uint64 // none of them can be empty
	compressor     *gzip.Writer
	compress_tmp   *bytes.Buffer
	hashfunc       hash.Hash
}

func (r *Resolver) NumContents() uint64 {
	if r.deduplicate {
		return uint64(len(r.OffsetMap))
	} else {
		return r.AddressedTiles
	}
}

// must be called in increasing tile_id order, uniquely
func (r *Resolver) AddTileIsNew(tile_id uint64, data []byte) (bool, []byte) {
	r.AddressedTiles++
	var found OffsetLen
	var ok bool
	var sum_string string
	if r.deduplicate {
		r.hashfunc.Reset()
		r.hashfunc.Write(data)
		var tmp []byte
		sum_string = string(r.hashfunc.Sum(tmp))
		found, ok = r.OffsetMap[sum_string]
	}

	if r.deduplicate && ok {
		last_entry := r.Entries[len(r.Entries)-1]
		if tile_id == last_entry.TileId+uint64(last_entry.RunLength) && last_entry.Offset == found.Offset {
			// RLE
			if last_entry.RunLength+1 > math.MaxUint32 {
				panic("Maximum 32-bit run length exceeded")
			}
			r.Entries[len(r.Entries)-1].RunLength++
		} else {
			r.Entries = append(r.Entries, EntryV3{tile_id, found.Offset, found.Length, 1})
		}

		return false, nil
	} else {
		var new_data []byte
		if !r.compress || (len(data) >= 2 && data[0] == 31 && data[1] == 139) {
			// the tile is already compressed
			new_data = data
		} else {
			r.compress_tmp.Reset()
			r.compressor.Reset(r.compress_tmp)
			r.compressor.Write(data)
			r.compressor.Close()
			new_data = r.compress_tmp.Bytes()
		}

		if r.deduplicate {
			r.OffsetMap[sum_string] = OffsetLen{r.Offset, uint32(len(new_data))}
		}
		r.Entries = append(r.Entries, EntryV3{tile_id, r.Offset, uint32(len(new_data)), 1})
		r.Offset += uint64(len(new_data))
		return true, new_data
	}
}

func NewResolver(deduplicate bool, compress bool) *Resolver {
	b := new(bytes.Buffer)
	compressor, _ := gzip.NewWriterLevel(b, gzip.BestCompression)
	r := Resolver{deduplicate, compress, make([]EntryV3, 0), 0, make(map[string]OffsetLen), 0, compressor, b, fnv.New128a()}
	return &r
}

func Convert(logger *log.Logger, input string, output string, deduplicate bool) error {
	if strings.HasSuffix(input, ".pmtiles") {
		return ConvertPmtilesV2(logger, input, output, deduplicate)
	} else {
		return ConvertMbtiles(logger, input, output, deduplicate)
	}
}

func add_directoryv2_entries(dir DirectoryV2, entries *[]EntryV3, f *os.File) {
	for zxy, rng := range dir.Entries {
		tile_id := ZxyToId(zxy.Z, zxy.X, zxy.Y)
		*entries = append(*entries, EntryV3{tile_id, rng.Offset, rng.Length, 1})
	}

	var unique = map[uint64]uint32{}

	// uniqify the offset/length pairs
	for _, rng := range dir.Leaves {
		unique[rng.Offset] = rng.Length
	}

	for offset, length := range unique {
		f.Seek(int64(offset), 0)
		leaf_bytes := make([]byte, length)
		f.Read(leaf_bytes)
		leaf_dir := ParseDirectoryV2(leaf_bytes)
		add_directoryv2_entries(leaf_dir, entries, f)
	}
}

func set_zoom_center_defaults(header *HeaderV3, entries []EntryV3) {
	min_z, _, _ := IdToZxy(entries[0].TileId)
	header.MinZoom = min_z
	max_z, _, _ := IdToZxy(entries[len(entries)-1].TileId)
	header.MaxZoom = max_z

	if header.CenterZoom == 0 && header.CenterLonE7 == 0 && header.CenterLatE7 == 0 {
		header.CenterZoom = header.MinZoom
		header.CenterLonE7 = (header.MinLonE7 + header.MaxLonE7) / 2
		header.CenterLatE7 = (header.MinLatE7 + header.MaxLatE7) / 2
	}
}

func ConvertPmtilesV2(logger *log.Logger, input string, output string, deduplicate bool) error {
	start := time.Now()
	f, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("Failed to open file: %w", err)
	}
	defer f.Close()
	buffer := make([]byte, 512000)
	io.ReadFull(f, buffer)
	if string(buffer[0:7]) == "PMTiles" && buffer[7] == 3 {
		return fmt.Errorf("Archive is already the latest PMTiles version (3).")
	}

	v2_json_bytes, dir := ParseHeaderV2(bytes.NewReader(buffer))

	var v2_metadata map[string]interface{}
	json.Unmarshal(v2_json_bytes, &v2_metadata)

	// get the first 4 bytes at offset 512000 to attempt tile type detection

	first4 := make([]byte, 4)
	f.Seek(512000, 0)
	n, err := f.Read(first4)
	if n != 4 || err != nil {
		return fmt.Errorf("Failed to read first 4, %w", err)
	}

	header, json_metadata, err := v2_to_header_json(v2_metadata, first4)

	if err != nil {
		return fmt.Errorf("Failed to convert v2 to header JSON, %w", err)
	}

	entries := make([]EntryV3, 0)
	add_directoryv2_entries(dir, &entries, f)

	// sort
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TileId < entries[j].TileId
	})

	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		return fmt.Errorf("Failed to create temp file, %w", err)
	}
	defer os.Remove(tmpfile.Name())

	// re-use resolver, because even if archives are de-duplicated we may need to recompress.
	resolver := NewResolver(deduplicate, header.TileType == Mvt)

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
		if is_new, new_data := resolver.AddTileIsNew(entry.TileId, buf); is_new {
			tmpfile.Write(new_data)
		}
		bar.Add(1)
	}

	finalize(logger, resolver, header, tmpfile, output, json_metadata)
	logger.Println("Finished in ", time.Since(start))

	return nil
}

func ConvertMbtiles(logger *log.Logger, input string, output string, deduplicate bool) error {
	start := time.Now()
	conn, err := sqlite.OpenConn(input, sqlite.OpenReadOnly)
	if err != nil {
		return fmt.Errorf("Failed to create database connection, %w", err)
	}
	defer conn.Close()

	mbtiles_metadata := make([]string, 0)
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
			mbtiles_metadata = append(mbtiles_metadata, stmt.ColumnText(0))
			mbtiles_metadata = append(mbtiles_metadata, stmt.ColumnText(1))
		}
	}
	header, json_metadata, err := mbtiles_to_header_json(mbtiles_metadata)

	if err != nil {
		return fmt.Errorf("Failed to convert MBTiles to header JSON, %w", err)
	}

	logger.Println("Querying total tile count...")
	// determine the count
	var total_tiles int64
	{
		stmt, _, err := conn.PrepareTransient("SELECT count(*) FROM tiles")
		if err != nil {
			return fmt.Errorf("Failed to create statement, %w", err)
		}
		defer stmt.Finalize()
		row, err := stmt.Step()
		if err != nil || !row {
			return fmt.Errorf("Failed to step row, %w", err)
		}
		total_tiles = stmt.ColumnInt64(0)
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

		bar := progressbar.Default(total_tiles)

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
			flipped_y := (1 << z) - 1 - y
			id := ZxyToId(z, x, flipped_y)
			tileset.Add(id)
			bar.Add(1)
		}
	}

	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		return fmt.Errorf("Failed to create temporary file, %w", err)
	}
	defer os.Remove(tmpfile.Name())

	logger.Println("Pass 2: writing tiles")
	resolver := NewResolver(deduplicate, header.TileType == Mvt)
	{
		bar := progressbar.Default(int64(tileset.GetCardinality()))
		i := tileset.Iterator()
		stmt := conn.Prep("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?")

		var raw_tile_tmp bytes.Buffer

		for i.HasNext() {
			id := i.Next()
			z, x, y := IdToZxy(id)
			flipped_y := (1 << z) - 1 - y

			stmt.BindInt64(1, int64(z))
			stmt.BindInt64(2, int64(x))
			stmt.BindInt64(3, int64(flipped_y))

			has_row, err := stmt.Step()
			if err != nil {
				return fmt.Errorf("Failed to step statement, %w", err)
			}
			if !has_row {
				return fmt.Errorf("Missing row")
			}

			reader := stmt.ColumnReader(0)
			raw_tile_tmp.Reset()
			raw_tile_tmp.ReadFrom(reader)
			data := raw_tile_tmp.Bytes()

			if len(data) > 0 {
				if is_new, new_data := resolver.AddTileIsNew(id, data); is_new {
					tmpfile.Write(new_data)
				}
			}

			stmt.ClearBindings()
			stmt.Reset()
			bar.Add(1)
		}
	}
	finalize(logger, resolver, header, tmpfile, output, json_metadata)
	logger.Println("Finished in ", time.Since(start))
	return nil
}

func finalize(logger *log.Logger, resolver *Resolver, header HeaderV3, tmpfile *os.File, output string, json_metadata map[string]interface{}) error {
	logger.Println("# of addressed tiles: ", resolver.AddressedTiles)
	logger.Println("# of tile entries (after RLE): ", len(resolver.Entries))
	logger.Println("# of tile contents: ", resolver.NumContents())

	header.AddressedTilesCount = resolver.AddressedTiles
	header.TileEntriesCount = uint64(len(resolver.Entries))
	header.TileContentsCount = resolver.NumContents()

	// assemble the final file
	outfile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("Failed to create %s, %w", output, err)
	}

	root_bytes, leaves_bytes, num_leaves := optimize_directories(resolver.Entries, 16384-HEADERV3_LEN_BYTES)

	if num_leaves > 0 {
		logger.Println("Root dir bytes: ", len(root_bytes))
		logger.Println("Leaves dir bytes: ", len(leaves_bytes))
		logger.Println("Num leaf dirs: ", num_leaves)
		logger.Println("Total dir bytes: ", len(root_bytes)+len(leaves_bytes))
		logger.Println("Average leaf dir bytes: ", len(leaves_bytes)/num_leaves)
		logger.Printf("Average bytes per addressed tile: %.2f\n", float64(len(root_bytes)+len(leaves_bytes))/float64(resolver.AddressedTiles))
	} else {
		logger.Println("Total dir bytes: ", len(root_bytes))
		logger.Printf("Average bytes per addressed tile: %.2f\n", float64(len(root_bytes))/float64(resolver.AddressedTiles))
	}

	var metadata_bytes []byte
	{
		metadata_bytes_uncompressed, err := json.Marshal(json_metadata)
		if err != nil {
			return fmt.Errorf("Failed to marshal metadata, %w", err)
		}
		var b bytes.Buffer
		w, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
		w.Write(metadata_bytes_uncompressed)
		w.Close()
		metadata_bytes = b.Bytes()
	}

	set_zoom_center_defaults(&header, resolver.Entries)

	header.Clustered = true
	header.InternalCompression = Gzip
	if header.TileType == Mvt {
		header.TileCompression = Gzip
	}

	header.RootOffset = HEADERV3_LEN_BYTES
	header.RootLength = uint64(len(root_bytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.MetadataLength = uint64(len(metadata_bytes))
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(leaves_bytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength
	header.TileDataLength = resolver.Offset

	header_bytes := serialize_header(header)

	outfile.Write(header_bytes)
	outfile.Write(root_bytes)
	outfile.Write(metadata_bytes)
	outfile.Write(leaves_bytes)
	tmpfile.Seek(0, 0)
	_, err = io.Copy(outfile, tmpfile)
	if err != nil {
		return fmt.Errorf("Failed to copy data to outfile, %w", err)
	}

	return nil
}

func v2_to_header_json(v2_json_metadata map[string]interface{}, first4 []byte) (HeaderV3, map[string]interface{}, error) {
	header := HeaderV3{}

	if val, ok := v2_json_metadata["bounds"]; ok {
		min_lon, min_lat, max_lon, max_lat, err := parse_bounds(val.(string))
		if err != nil {
			return header, v2_json_metadata, err
		}
		header.MinLonE7 = min_lon
		header.MinLatE7 = min_lat
		header.MaxLonE7 = max_lon
		header.MaxLatE7 = max_lat
		delete(v2_json_metadata, "bounds")
	} else {
		return header, v2_json_metadata, errors.New("Archive is missing bounds.")
	}

	if val, ok := v2_json_metadata["center"]; ok {
		center_lon, center_lat, center_zoom, err := parse_center(val.(string))
		if err != nil {
			return header, v2_json_metadata, err
		}
		header.CenterLonE7 = center_lon
		header.CenterLatE7 = center_lat
		header.CenterZoom = center_zoom
		delete(v2_json_metadata, "center")
	}

	if val, ok := v2_json_metadata["compression"]; ok {
		switch val.(string) {
		case "gzip":
			header.TileCompression = Gzip
		default:
			return header, v2_json_metadata, errors.New("Unknown compression type")
		}
	} else {
		if first4[0] == 0x1f && first4[1] == 0x8b {
			header.TileCompression = Gzip
		}
	}

	if val, ok := v2_json_metadata["format"]; ok {
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
		default:
			return header, v2_json_metadata, errors.New("Unknown tile type")
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
	if val, ok := v2_json_metadata["json"]; ok {
		string_val := val.(string)
		var inside map[string]interface{}
		json.Unmarshal([]byte(string_val), &inside)
		for k, v := range inside {
			v2_json_metadata[k] = v
		}
		delete(v2_json_metadata, "json")
	}

	return header, v2_json_metadata, nil
}

func parse_bounds(bounds string) (int32, int32, int32, int32, error) {
	parts := strings.Split(bounds, ",")
	E7 := 10000000.0
	min_lon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	min_lat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	max_lon, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	max_lat, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return int32(min_lon * E7), int32(min_lat * E7), int32(max_lon * E7), int32(max_lat * E7), nil
}

func parse_center(center string) (int32, int32, uint8, error) {
	parts := strings.Split(center, ",")
	E7 := 10000000.0
	center_lon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	center_lat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, 0, 0, err
	}
	center_zoom, err := strconv.ParseInt(parts[2], 10, 8)
	if err != nil {
		return 0, 0, 0, err
	}
	return int32(center_lon * E7), int32(center_lat * E7), uint8(center_zoom), nil
}

func mbtiles_to_header_json(mbtiles_metadata []string) (HeaderV3, map[string]interface{}, error) {
	header := HeaderV3{}
	json_result := make(map[string]interface{})
	for i := 0; i < len(mbtiles_metadata); i += 2 {
		value := mbtiles_metadata[i+1]
		switch key := mbtiles_metadata[i]; key {
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
			}
			json_result["format"] = value
		case "bounds":
			min_lon, min_lat, max_lon, max_lat, err := parse_bounds(value)
			if err != nil {
				return header, json_result, err
			}
			header.MinLonE7 = min_lon
			header.MinLatE7 = min_lat
			header.MaxLonE7 = max_lon
			header.MaxLatE7 = max_lat
		case "center":
			center_lon, center_lat, center_zoom, err := parse_center(value)
			if err != nil {
				return header, json_result, err
			}
			header.CenterLonE7 = center_lon
			header.CenterLatE7 = center_lat
			header.CenterZoom = center_zoom
		case "json":
			var mbtiles_json map[string]interface{}
			json.Unmarshal([]byte(value), &mbtiles_json)
			for k, v := range mbtiles_json {
				json_result[k] = v
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
			json_result["compression"] = value
		// name, attribution, description, type, version
		default:
			json_result[key] = value
		}
	}
	return header, json_result, nil
}
