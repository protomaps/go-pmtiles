package pmtiles

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/maptile/tilecover"
	"github.com/paulmach/orb/planar"
	"github.com/paulmach/orb/project"
	"image"
	"image/color"
	"image/png"
	"log"
	"os"
)

func bitmapMultiPolygon(zoom uint8, multipolygon orb.MultiPolygon) (*roaring64.Bitmap, *roaring64.Bitmap) {
	boundary_set := roaring64.New()

	for _, polygon := range multipolygon {
		for _, ring := range polygon {
			boundary_tiles, _ := tilecover.Geometry(orb.LineString(ring), maptile.Zoom(zoom)) // TODO is this buffer-aware?
			for tile := range boundary_tiles {
				boundary_set.Add(ZxyToId(uint8(tile.Z), tile.X, tile.Y))
			}
		}
	}

	multipolygon_projected := project.MultiPolygon(multipolygon.Clone(), project.WGS84.ToMercator)

	interior_set := roaring64.New()
	i := boundary_set.Iterator()
	for i.HasNext() {
		id := i.Next()
		if !boundary_set.Contains(id+1) && i.HasNext() {
			z, x, y := IdToZxy(id + 1)
			tile := maptile.New(x, y, maptile.Zoom(z))
			if planar.MultiPolygonContains(multipolygon_projected, project.Point(tile.Center(), project.WGS84.ToMercator)) {
				interior_set.AddRange(id+1, i.PeekNext())
			}
		}
	}

	return boundary_set, interior_set
}

func generalizeOr(r *roaring64.Bitmap) {
	if r.GetCardinality() == 0 {
		return
	}
	max_z, _, _ := IdToZxy(r.ReverseIterator().Next())

	var temp *roaring64.Bitmap
	var to_iterate *roaring64.Bitmap

	temp = roaring64.New()
	to_iterate = r

	for current_z := int(max_z); current_z > 0; current_z-- {
		iter := to_iterate.Iterator()
		for iter.HasNext() {
			parent_id := ParentId(iter.Next())
			temp.Add(parent_id)
		}
		to_iterate = temp
		r.Or(temp)
		temp = roaring64.New()
	}
}

func generalizeAnd(r *roaring64.Bitmap) {
	if r.GetCardinality() == 0 {
		return
	}
	max_z, _, _ := IdToZxy(r.ReverseIterator().Next())

	var temp *roaring64.Bitmap
	var to_iterate *roaring64.Bitmap

	temp = roaring64.New()
	to_iterate = r

	for current_z := int(max_z); current_z > 0; current_z-- {
		iter := to_iterate.Iterator()
		filled := 0
		current := uint64(0) // check me...
		for iter.HasNext() {
			id := iter.Next()
			parent_id := ParentId(id)
			if parent_id == current {
				filled += 1
				if filled == 4 {
					temp.Add(parent_id)
				}
			} else {
				current = parent_id
				filled = 1
			}
		}
		to_iterate = temp
		r.Or(temp)
		temp = roaring64.New()
	}
}

func WriteImage(interior *roaring64.Bitmap, boundary *roaring64.Bitmap, exterior *roaring64.Bitmap, filename string, zoom uint8) {
	dim := 1 << zoom
	img := image.NewNRGBA(image.Rect(0, 0, dim, dim))

	min := ZxyToId(zoom, 0, 0)
	max := ZxyToId(zoom+1, 0, 0)

	{
		iter := interior.Iterator()
		fill := color.NRGBA{R: 0, G: 255, B: 255, A: 255}
		for iter.HasNext() {
			id := iter.Next()
			if id >= min && id < max {
				_, x, y := IdToZxy(id)
				img.Set(int(x), int(y), fill)
			}
		}
	}
	{
		iter := boundary.Iterator()
		fill := color.NRGBA{R: 255, G: 0, B: 255, A: 255}
		for iter.HasNext() {
			id := iter.Next()
			if id >= min && id < max {
				_, x, y := IdToZxy(id)
				img.Set(int(x), int(y), fill)
			}
		}
	}
	{
		iter := exterior.Iterator()
		fill := color.NRGBA{R: 255, G: 255, B: 0, A: 255}
		for iter.HasNext() {
			id := iter.Next()
			if id >= min && id < max {
				_, x, y := IdToZxy(id)
				img.Set(int(x), int(y), fill)
			}
		}
	}

	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}

	if err := png.Encode(f, img); err != nil {
		f.Close()
		log.Fatal(err)
	}

	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}
