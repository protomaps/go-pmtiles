package pmtiles

import (
	"encoding/json"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/schollz/progressbar/v3"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"zombiezen.com/go/sqlite"
)

func Convert(logger *log.Logger, input string, output string) {
	conn, err := sqlite.OpenConn(input, sqlite.OpenReadOnly)
	if err != nil {
		logger.Fatal(err)
	}
	defer conn.Close()

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
			if reader != nil {

			}

			stmt.ClearBindings()
			stmt.Reset()
			bar.Add(1)
		}
	}

	mbtilesMetadata := make([]string, 0)
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
			mbtilesMetadata = append(mbtilesMetadata, stmt.ColumnText(0))
			mbtilesMetadata = append(mbtilesMetadata, stmt.ColumnText(1))
		}
	}
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
