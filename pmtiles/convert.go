package pmtiles

import (
	"bytes"
	"compress/gzip"
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
	Entries        []EntryV3
	Offset         uint64
	OffsetMap      map[string]uint64
	AddressedTiles uint64 // none of them can be empty
}

// must be called in increasing tile_id order, uniquely
func (r *Resolver) AddTileIsNew(tile_id uint64, data []byte) bool {
	r.AddressedTiles++
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
	return &Resolver{make([]EntryV3, 0), 0, make(map[string]uint64), 0}
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
				logger.Fatal(err)
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

	logger.Println("Querying total tile count...")
	// determine the count
	var total_tiles int64
	{
		stmt, _, err := conn.PrepareTransient("SELECT count(*) FROM tiles")
		if err != nil {
			logger.Fatal(err)
		}
		defer stmt.Finalize()
		row, err := stmt.Step()
		if err != nil || !row {
			logger.Fatal(err)
		}
		total_tiles = stmt.ColumnInt64(0)
	}

	logger.Println("Pass 1: Assembling TileID set")
	// assemble a sorted set of all TileIds
	tileset := roaring64.New()
	{
		stmt, _, err := conn.PrepareTransient("SELECT zoom_level, tile_column, tile_row FROM tiles")
		if err != nil {
			logger.Fatal(err)
		}
		defer stmt.Finalize()

		bar := progressbar.Default(total_tiles)

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
			bar.Add(1)
		}
	}

	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		logger.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	logger.Println("Pass 2: writing tiles")
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

			if len(data) == 0 {
				continue
			}

			if len(data) >= 2 && data[0] == 31 && data[1] == 139 {
				// the tile is already gzipped
			} else {
				var b bytes.Buffer
				w, _ := gzip.NewWriterLevel(&b, gzip.DefaultCompression)
				w.Write(data)
				w.Close()
				data = b.Bytes()
			}

			if resolver.AddTileIsNew(id, data) {
				tmpfile.Write(data)
			}

			stmt.ClearBindings()
			stmt.Reset()
			bar.Add(1)
		}
	}

	logger.Println("# of addressed tiles: ", resolver.AddressedTiles)
	logger.Println("# of tile entries (after RLE): ", len(resolver.Entries))
	logger.Println("# of tile contents: ", len(resolver.OffsetMap))

	header.AddressedTilesCount = resolver.AddressedTiles
	header.TileEntriesCount = uint64(len(resolver.Entries))
	header.TileContentsCount = uint64(len(resolver.OffsetMap))

	// assemble the final file
	outfile, err := os.Create(output)
	if err != nil {
		logger.Fatal(err)
	}

	root_bytes, leaves_bytes, num_leaves := optimize_directories(resolver.Entries, 16384-HEADERV3_LEN_BYTES)

	if num_leaves > 0 {
		logger.Println("Root dir bytes: ", len(root_bytes))
		logger.Println("Leaves dir bytes: ", len(leaves_bytes))
		logger.Println("Num leaf dirs: ", num_leaves)
		logger.Println("Total dir bytes: ", len(root_bytes)+len(leaves_bytes))
		logger.Println("Average leaf dir bytes: ", len(leaves_bytes)/num_leaves)
		logger.Printf("Average bytes per entry: %.2f\n", float64(len(root_bytes)+len(leaves_bytes))/float64(resolver.AddressedTiles))
	} else {
		logger.Println("Total dir bytes: ", len(root_bytes))
		logger.Printf("Average bytes per entry: %.2f\n", float64(len(root_bytes))/float64(resolver.AddressedTiles))
	}

	var metadata_bytes []byte
	{
		metadata_bytes_uncompressed, err := json.Marshal(json_metadata)
		if err != nil {
			logger.Fatal(err)
		}
		var b bytes.Buffer
		w, _ := gzip.NewWriterLevel(&b, gzip.DefaultCompression)
		w.Write(metadata_bytes_uncompressed)
		w.Close()
		metadata_bytes = b.Bytes()
	}

	header.Clustered = true
	header.InternalCompression = Gzip
	header.TileCompression = Gzip

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
		logger.Fatal(err)
	}

	logger.Println("Finished in ", time.Since(start))
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
			f, err := strconv.ParseFloat(parts[0], 64)
			if err != nil {
				return header, json_result, err
			}
			header.MinLonE7 = int32(f * 10000000)
			f, err = strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return header, json_result, err
			}
			header.MinLatE7 = int32(f * 10000000)
			f, err = strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return header, json_result, err
			}
			header.MaxLonE7 = int32(f * 10000000)
			f, err = strconv.ParseFloat(parts[3], 64)
			if err != nil {
				return header, json_result, err
			}
			header.MaxLatE7 = int32(f * 10000000)
		case "center":
			parts := strings.Split(value, ",")
			f, err := strconv.ParseFloat(parts[0], 64)
			if err != nil {
				return header, json_result, err
			}
			header.CenterLonE7 = int32(f * 10000000)
			f, err = strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return header, json_result, err
			}
			header.CenterLatE7 = int32(f * 10000000)
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
