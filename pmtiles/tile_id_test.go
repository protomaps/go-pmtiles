package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestZxyToId(t *testing.T) {
	id := ZxyToId(0, 0, 0)
	if id != 0 {
		t.Fatalf(`expected %d to be 0`, id)
	}
	id = ZxyToId(1, 0, 0)
	if id != 1 {
		t.Fatalf(`expected %d to be 1`, id)
	}
	id = ZxyToId(1, 0, 1)
	if id != 2 {
		t.Fatalf(`expected %d to be 2`, id)
	}
	id = ZxyToId(1, 1, 1)
	if id != 3 {
		t.Fatalf(`expected %d to be 3`, id)
	}
	id = ZxyToId(1, 1, 0)
	if id != 4 {
		t.Fatalf(`expected %d to be 4`, id)
	}
	id = ZxyToId(2, 0, 0)
	if id != 5 {
		t.Fatalf(`expected %d to be 5`, id)
	}
}

func TestIdToZxy(t *testing.T) {
	z, x, y := IdToZxy(0)
	if !(z == 0 && x == 0 && y == 0) {
		t.Fatalf(`expected to be (0,0,0)`)
	}
	z, x, y = IdToZxy(1)
	if !(z == 1 && x == 0 && y == 0) {
		t.Fatalf(`expected to be (1,0,0)`)
	}
	z, x, y = IdToZxy(19078479)
	if !(z == 12 && x == 3423 && y == 1763) {
		t.Fatalf(`expected to be (12,3423,1763)`)
	}
}

func TestManyTileIds(t *testing.T) {
	var z uint8
	var x uint32
	var y uint32
	for z = 0; z < 10; z++ {
		for x = 0; x < (1 << z); x++ {
			for y = 0; y < (1 << z); y++ {
				id := ZxyToId(z, x, y)
				rz, rx, ry := IdToZxy(id)
				if !(z == rz && x == rx && y == ry) {
					t.Fatalf(`fail on %d %d %d`, z, x, y)
				}
			}
		}
	}
}

func TestExtremes(t *testing.T) {
	var tz uint8
	for tz = 0; tz < 32; tz++ {
		var dim uint32
		dim = (1 << tz) - 1
		z, x, y := IdToZxy(ZxyToId(tz, 0, 0))
		assert.Equal(t, tz, z)
		assert.Equal(t, uint32(0), x)
		assert.Equal(t, uint32(0), y)
		z, x, y = IdToZxy(ZxyToId(z, dim, 0))
		assert.Equal(t, tz, z)
		assert.Equal(t, dim, x)
		assert.Equal(t, uint32(0), y)
		z, x, y = IdToZxy(ZxyToId(z, 0, dim))
		assert.Equal(t, tz, z)
		assert.Equal(t, uint32(0), x)
		assert.Equal(t, dim, y)
		z, x, y = IdToZxy(ZxyToId(z, dim, dim))
		assert.Equal(t, tz, z)
		assert.Equal(t, dim, x)
		assert.Equal(t, dim, y)
	}
}
