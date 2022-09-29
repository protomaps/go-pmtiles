package pmtiles

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/schollz/progressbar/v3"
	"io/ioutil"
	"log"
	"os"
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
