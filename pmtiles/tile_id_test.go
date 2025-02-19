package pmtiles

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		var dim uint32 = (1 << tz) - 1
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

	assert.Equal(t, ZxyToID(18, 500, 1), ParentID(ZxyToID(19, 1000, 3)))
	assert.Equal(t, ZxyToID(18, 500, 2), ParentID(ZxyToID(19, 1000, 4)))
	assert.Equal(t, ZxyToID(18, 1, 500), ParentID(ZxyToID(19, 3, 1000)))
	assert.Equal(t, ZxyToID(18, 2, 500), ParentID(ZxyToID(19, 4, 1000)))
}

func BenchmarkZxyToId(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for z := uint8(0); z < 15; z += 1 {
			s := uint32(1 << z)
			for x := uint32(0); x < s; x += 13 {
				for y := uint32(0); y < s; y += 13 {
					_ = ZxyToID(z, x, y)
				}
			}
		}
	}
}

func BenchmarkIdToZxy(b *testing.B) {
	end := ZxyToID(15, 0, 0)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := uint64(0); i < end; i += 13 {
			_, _, _ = IDToZxy(i)
		}
	}
}

func BenchmarkParentId(b *testing.B) {
	end := ZxyToID(15, 0, 0)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := uint64(1); i < end; i += 13 {
			_ = ParentID(i)
		}
	}
}
