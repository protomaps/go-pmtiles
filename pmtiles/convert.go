package pmtiles

import (
	"bytes"
	"encoding/json"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/schollz/progressbar/v3"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"zombiezen.com/go/sqlite"
)

type Resolver struct {
	Entries   []EntryV3
	Offset    uint64
	OffsetMap map[string]uint64
}

// must be called in increasing tile_id order
func (r *Resolver) AddTileIsNew(tile_id uint64, data []byte) bool {
	hashfunc := fnv.New128a()
	hashfunc.Write(data)
	var tmp []byte
	sum := hashfunc.Sum(tmp)
	sum_string := string(sum)

	if found_offset, ok := r.OffsetMap[sum_string]; ok {
		last_entry := r.Entries[len(r.Entries)-1]
		if tile_id == last_entry.TileId+uint64(last_entry.RunLength) && last_entry.Offset == found_offset {
			// RLE
			if last_entry.RunLength+1 > math.MaxUint32 {
				panic("Maximum 32-bit run length exceeded")
			}
			r.Entries[len(r.Entries)-1].RunLength++
		} else {
			r.Entries = append(r.Entries, EntryV3{tile_id, found_offset, uint32(len(data)), 1})
		}

		return false
	} else {
		r.OffsetMap[sum_string] = r.Offset
		r.Entries = append(r.Entries, EntryV3{tile_id, r.Offset, uint32(len(data)), 1})
		r.Offset += uint64(len(data))
		return true
	}
}

func NewResolver() *Resolver {
	return &Resolver{make([]EntryV3, 0), 0, make(map[string]uint64)}
}

func Convert(logger *log.Logger, input string, output string) {
	start := time.Now()
	conn, err := sqlite.OpenConn(input, sqlite.OpenReadOnly)
	if err != nil {
		logger.Fatal(err)
	}
	defer conn.Close()

	mbtiles_metadata := make([]string, 0)
	{
		stmt, _, err := conn.PrepareTransient("SELECT name, value FROM metadata")
		if err != nil {
			logger.Fatal(err)
		}
		defer stmt.Finalize()

		for {
			row, err := stmt.Step()
			if err != nil {
				log.Fatal(err)
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
		logger.Fatal(err)
	}

	// assemble a sorted set of all TileIds
	tileset := roaring64.New()
	{
		stmt, _, err := conn.PrepareTransient("SELECT zoom_level, tile_column, tile_row FROM tiles")
		if err != nil {
			logger.Fatal(err)
		}
		defer stmt.Finalize()

		for {
			row, err := stmt.Step()
			if err != nil {
				logger.Fatal(err)
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
		}
	}

	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		logger.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	// write tiles to tmpfile with deduplication
	resolver := NewResolver()
	{
		bar := progressbar.Default(int64(tileset.GetCardinality()))
		i := tileset.Iterator()
		stmt := conn.Prep("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?")

		for i.HasNext() {
			id := i.Next()
			z, x, y := IdToZxy(id)
			flipped_y := (1 << z) - 1 - y

			stmt.BindInt64(1, int64(z))
			stmt.BindInt64(2, int64(x))
			stmt.BindInt64(3, int64(flipped_y))

			has_row, err := stmt.Step()
			if err != nil {
				logger.Fatal(err)
			}
			if !has_row {
				logger.Fatal("Missing row")
			}

			reader := stmt.ColumnReader(0)
			buf := new(bytes.Buffer)
			buf.ReadFrom(reader)
			data := buf.Bytes()

			if resolver.AddTileIsNew(id, data) {
				tmpfile.Write(data)
			}

			stmt.ClearBindings()
			stmt.Reset()
			bar.Add(1)
		}
	}

	logger.Println("# of addressed tiles: ", tileset.GetCardinality())
	logger.Println("# of tile entries (after RLE): ", len(resolver.Entries))
	logger.Println("# of tile contents: ", len(resolver.OffsetMap))

	header.AddressedTilesCount = tileset.GetCardinality()
	header.TileEntriesCount = uint64(len(resolver.Entries))
	header.TileContentsCount = uint64(len(resolver.OffsetMap))

	// assemble the final file
	outfile, err := os.Create(os.Args[2])

	root_bytes, leaves_bytes := optimize_directories(resolver.Entries, 16384-HEADERV3_LEN_BYTES)

	metadata_bytes, err := json.Marshal(json_metadata)
	if err != nil {
		log.Fatal(err)
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
	io.Copy(outfile, tmpfile)

	logger.Println("Finished in ", time.Since(start))
}

func optimize_directories(entries []EntryV3, target_root_len int) ([]byte, []byte) {
	root_bytes := make([]byte, 0)
	leaves_bytes := make([]byte, 0)
	return root_bytes, leaves_bytes
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
			case "jpg":
				header.TileType = Jpeg
			case "webp":
				header.TileType = Webp
			}
			json_result["format"] = value
		case "bounds":
			parts := strings.Split(value, ",")
			f, err := strconv.ParseFloat(parts[0], 32)
			if err != nil {
				return header, json_result, err
			}
			header.MinLon = float32(f)
			f, err = strconv.ParseFloat(parts[1], 32)
			if err != nil {
				return header, json_result, err
			}
			header.MinLat = float32(f)
			f, err = strconv.ParseFloat(parts[2], 32)
			if err != nil {
				return header, json_result, err
			}
			header.MaxLon = float32(f)
			f, err = strconv.ParseFloat(parts[3], 32)
			if err != nil {
				return header, json_result, err
			}
			header.MaxLat = float32(f)
		case "center":
			parts := strings.Split(value, ",")
			f, err := strconv.ParseFloat(parts[0], 32)
			if err != nil {
				return header, json_result, err
			}
			header.CenterLon = float32(f)
			f, err = strconv.ParseFloat(parts[1], 32)
			if err != nil {
				return header, json_result, err
			}
			header.CenterLat = float32(f)
			i, err := strconv.ParseInt(parts[2], 10, 8)
			if err != nil {
				return header, json_result, err
			}
			header.CenterZoom = uint8(i)
		case "minzoom":
			i, err := strconv.ParseInt(value, 10, 8)
			if err != nil {
				return header, json_result, err
			}
			header.MinZoom = uint8(i)
		case "maxzoom":
			i, err := strconv.ParseInt(value, 10, 8)
			if err != nil {
				return header, json_result, err
			}
			header.MaxZoom = uint8(i)
		case "json":
			var mbtiles_json map[string]interface{}
			json.Unmarshal([]byte(value), &mbtiles_json)
			for k, v := range mbtiles_json {
				json_result[k] = v
			}
		case "compression":
			switch value {
			case "gzip":
				header.TileCompression = Gzip
			}
			json_result["compression"] = value
		// name, attribution, description, type, version
		default:
			json_result[key] = value
		}
	}
	return header, json_result, nil
}
