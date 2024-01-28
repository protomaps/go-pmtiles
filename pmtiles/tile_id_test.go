package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestZxyToId(t *testing.T) {
	assert.Equal(t, uint64(0), ZxyToID(0, 0, 0))
	assert.Equal(t, uint64(1), ZxyToID(1, 0, 0))
	assert.Equal(t, uint64(2), ZxyToID(1, 0, 1))
	assert.Equal(t, uint64(3), ZxyToID(1, 1, 1))
	assert.Equal(t, uint64(4), ZxyToID(1, 1, 0))
	assert.Equal(t, uint64(5), ZxyToID(2, 0, 0))
}

func TestIdToZxy(t *testing.T) {
	z, x, y := IDToZxy(0)
	assert.Equal(t, uint8(0), z)
	assert.Equal(t, uint32(0), x)
	assert.Equal(t, uint32(0), y)
	z, x, y = IDToZxy(1)
	assert.Equal(t, uint8(1), z)
	assert.Equal(t, uint32(0), x)
	assert.Equal(t, uint32(0), y)
	z, x, y = IDToZxy(19078479)
	assert.Equal(t, uint8(12), z)
	assert.Equal(t, uint32(3423), x)
	assert.Equal(t, uint32(1763), y)
}

func TestManyTileIds(t *testing.T) {
	var z uint8
	var x uint32
	var y uint32
	for z = 0; z < 10; z++ {
		for x = 0; x < (1 << z); x++ {
			for y = 0; y < (1 << z); y++ {
				id := ZxyToID(z, x, y)
				rz, rx, ry := IDToZxy(id)
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
		z, x, y := IDToZxy(ZxyToID(tz, 0, 0))
		assert.Equal(t, tz, z)
		assert.Equal(t, uint32(0), x)
		assert.Equal(t, uint32(0), y)
		z, x, y = IDToZxy(ZxyToID(z, dim, 0))
		assert.Equal(t, tz, z)
		assert.Equal(t, dim, x)
		assert.Equal(t, uint32(0), y)
		z, x, y = IDToZxy(ZxyToID(z, 0, dim))
		assert.Equal(t, tz, z)
		assert.Equal(t, uint32(0), x)
		assert.Equal(t, dim, y)
		z, x, y = IDToZxy(ZxyToID(z, dim, dim))
		assert.Equal(t, tz, z)
		assert.Equal(t, dim, x)
		assert.Equal(t, dim, y)
	}
}

func TestParent(t *testing.T) {
	assert.Equal(t, ZxyToID(0, 0, 0), ParentID(ZxyToID(1, 0, 0)))

	assert.Equal(t, ZxyToID(1, 0, 0), ParentID(ZxyToID(2, 0, 0)))
	assert.Equal(t, ZxyToID(1, 0, 0), ParentID(ZxyToID(2, 0, 1)))
	assert.Equal(t, ZxyToID(1, 0, 0), ParentID(ZxyToID(2, 1, 0)))
	assert.Equal(t, ZxyToID(1, 0, 0), ParentID(ZxyToID(2, 1, 1)))

	assert.Equal(t, ZxyToID(1, 0, 1), ParentID(ZxyToID(2, 0, 2)))
	assert.Equal(t, ZxyToID(1, 0, 1), ParentID(ZxyToID(2, 0, 3)))
	assert.Equal(t, ZxyToID(1, 0, 1), ParentID(ZxyToID(2, 1, 2)))
	assert.Equal(t, ZxyToID(1, 0, 1), ParentID(ZxyToID(2, 1, 3)))

	assert.Equal(t, ZxyToID(1, 1, 0), ParentID(ZxyToID(2, 2, 0)))
	assert.Equal(t, ZxyToID(1, 1, 0), ParentID(ZxyToID(2, 2, 1)))
	assert.Equal(t, ZxyToID(1, 1, 0), ParentID(ZxyToID(2, 3, 0)))
	assert.Equal(t, ZxyToID(1, 1, 0), ParentID(ZxyToID(2, 3, 1)))

	assert.Equal(t, ZxyToID(1, 1, 1), ParentID(ZxyToID(2, 2, 2)))
	assert.Equal(t, ZxyToID(1, 1, 1), ParentID(ZxyToID(2, 2, 3)))
	assert.Equal(t, ZxyToID(1, 1, 1), ParentID(ZxyToID(2, 3, 2)))
	assert.Equal(t, ZxyToID(1, 1, 1), ParentID(ZxyToID(2, 3, 3)))
}
