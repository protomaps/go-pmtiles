package pmtiles

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGeneralizeAnd(t *testing.T) {
	b := roaring64.New()
	generalizeAnd(b)
	assert.Equal(t, uint64(0), b.GetCardinality())
	b = roaring64.New()
	b.Add(ZxyToID(3, 0, 0))
	generalizeAnd(b)
	assert.Equal(t, uint64(1), b.GetCardinality())
	b = roaring64.New()
	b.Add(ZxyToID(3, 0, 0))
	b.Add(ZxyToID(3, 0, 1))
	b.Add(ZxyToID(3, 1, 0))
	b.Add(ZxyToID(3, 1, 1))
	generalizeAnd(b)
	assert.Equal(t, uint64(5), b.GetCardinality())
	assert.True(t, b.Contains(ZxyToID(2, 0, 0)))
}

func TestGeneralizeOr(t *testing.T) {
	b := roaring64.New()
	generalizeOr(b, 0)
	assert.Equal(t, uint64(0), b.GetCardinality())
	b = roaring64.New()
	b.Add(ZxyToID(3, 0, 0))
	generalizeOr(b, 0)
	assert.Equal(t, uint64(4), b.GetCardinality())
	assert.True(t, b.Contains(ZxyToID(2, 0, 0)))
	assert.True(t, b.Contains(ZxyToID(1, 0, 0)))
	assert.True(t, b.Contains(ZxyToID(0, 0, 0)))
}

func TestGeneralizeOrMinZoom(t *testing.T) {
	b := roaring64.New()
	b.Add(ZxyToID(3, 0, 0))
	generalizeOr(b, 2)
	assert.Equal(t, uint64(2), b.GetCardinality())
	assert.True(t, b.Contains(ZxyToID(2, 0, 0)))
	assert.False(t, b.Contains(ZxyToID(1, 0, 0)))
}
