package pmtiles

import (
	"encoding/json"
	"io"
	"log"
	"math"
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

func PointToTile(z int, lng float64, lat float64) Zxy {
	d2r := math.Pi / 180
	sin := math.Sin(lat * d2r)
	z2 := 1 << z
	x := float64(z2) * (lng/360 + 0.5)
	y := float64(z2) * (0.5 - 0.25*math.Log((1+sin)/(1-sin))/math.Pi)
	x = math.Mod(x, float64(z2))
	if x < 0 {
		x = x + float64(z2)
	}
	iy := int(math.Floor(y))
	if iy > z2-1 {
		iy = z2 - 1
	}
	return Zxy{Z: uint8(z), X: uint32(math.Floor(x)), Y: uint32(iy)}
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

func SubpyramidXY(logger *log.Logger, input string, output string, z uint8, minX uint32, minY uint32, maxX uint32, maxY uint32, bounds string) {
	f, err := os.Open(input)
	if err != nil {
		return
	}
	metadata_bytes, root_directory := ParseHeaderV2(f)

	var metadata Metadata
	json.Unmarshal(metadata_bytes, &metadata)
	metadata.Maxzoom = strconv.Itoa(int(z))
	metadata.Bounds = bounds

	// writer := NewWriter(output)

	if z >= root_directory.LeafZ {
		for key, rng := range root_directory.Leaves {
			if Matches(z, minX, minY, maxX, maxY, key) {
				_, err = f.Seek(int64(rng.Offset), 0)
				if err != nil {
					panic("I/O error")
				}

				dir_bytes := make([]byte, rng.Length)
				io.ReadFull(f, dir_bytes)

				for i := 0; i < len(dir_bytes)/17; i++ {
					leaf_z, lzxy, lrng := ParseEntryV2(dir_bytes[i*17 : i*17+17])
					if leaf_z == 0 {
						if lzxy.Z <= z && Matches(z, minX, minY, maxX, maxY, lzxy) {
							_, err = f.Seek(int64(lrng.Offset), 0)
							if err != nil {
								return
							}
							tile_data := make([]byte, lrng.Length)
							io.ReadFull(f, tile_data)
							// writer.WriteTile(lzxy, tile_data)
						}
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
			// writer.WriteTile(key, tile_data)
		}
	}

	new_metadata_bytes, _ := json.Marshal(metadata)
	_ = new_metadata_bytes
	// writer.Finalize(new_metadata_bytes)
}
