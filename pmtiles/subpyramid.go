package pmtiles

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
)

type Metadata struct {
	Format      string `json:"format"`
	Minzoom     string `json:"minzoom"`
	Maxzoom     string `json:"maxzoom"`
	Bounds      string `json:"bounds"`
	Compress    string `json:"compress"`
	Attribution string `json:"attribution"`
}

// given a source PMTiles archive,
// extract a subpyramid with the following parameters
// ZOOM_LEVEL, min_x, min_y, max_x, max_y
// Todo: add or replace the MASK layer with one feature

func Matches(z uint8, minX uint32, minY uint32, maxX uint32, maxY uint32, candidate Zxy) bool {
	if candidate.Z < z {
		levels := z - candidate.Z
		candidateMinXOnLevel := candidate.X << levels
		candidateMinYOnLevel := candidate.Y << levels
		candidateMaxXOnLevel := ((candidate.X + 1) << levels) - 1
		candidateMaxYOnLevel := ((candidate.Y + 1) << levels) - 1
		if candidateMaxXOnLevel < minX || candidateMaxYOnLevel < minY || candidateMinXOnLevel > maxX || candidateMinYOnLevel > maxY {
			return false
		}
		return true
	} else if candidate.Z == z {
		return candidate.X >= minX && candidate.Y >= minY && candidate.X <= maxX && candidate.Y <= maxY
	} else {
		levels := candidate.Z - z
		candidateXOnLevel := candidate.X >> levels
		candidateYOnLevel := candidate.Y >> levels
		return candidateXOnLevel >= minX && candidateYOnLevel >= minY && candidateXOnLevel <= maxX && candidateYOnLevel <= maxY
	}
}

func Subpyramid(logger *log.Logger, input string, output string, z uint8, minX uint32, minY uint32, maxX uint32, maxY uint32) {
	f, err := os.Open(input)
	if err != nil {
		return
	}
	logger.Println(f)
	metadata_bytes, root_directory := ParseHeader(f)

	var metadata Metadata
	json.Unmarshal(metadata_bytes, &metadata)
	metadata.Maxzoom = strconv.Itoa(int(z))

	writer := NewWriter(output)

	if z >= root_directory.LeafZ {
		for key, rng := range root_directory.Leaves {
			if Matches(z, minX, minY, maxX, maxY, key) {
				_, err = f.Seek(int64(rng.Offset), 0)
				if err != nil {
					panic("I/O error")
				}
				dir := ParseDirectory(bufio.NewReaderSize(f, int(rng.Length)), rng.Length/17)
				for lkey, lrng := range dir.Entries {
					if lkey.Z <= z && Matches(z, minX, minY, maxX, maxY, lkey) {
						_, err = f.Seek(int64(lrng.Offset), 0)
						if err != nil {
							return
						}
						tile_data := make([]byte, lrng.Length)
						io.ReadFull(f, tile_data)
						writer.WriteTile(lkey, tile_data)
					}
				}
			}
		}
	}
	for key, rng := range root_directory.Entries {
		if Matches(z, minX, minY, maxX, maxY, key) {
			_, err = f.Seek(int64(rng.Offset), 0)
			if err != nil {
				return
			}
			tile_data := make([]byte, rng.Length)
			io.ReadFull(f, tile_data)
			writer.WriteTile(key, tile_data)
		}
	}

	new_metadata_bytes, _ := json.Marshal(metadata)
	writer.Finalize(new_metadata_bytes)
}
