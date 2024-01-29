package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUint24(t *testing.T) {
	b := []byte{255, 255, 255}
	assert.Equal(t, uint32(16777215), readUint24(b))
	b = []byte{255, 0, 0}
	assert.Equal(t, uint32(255), readUint24(b))
}

func TestUint48(t *testing.T) {
	b := []byte{255, 255, 255, 255, 255, 255}
	assert.Equal(t, uint64(281474976710655), readUint48(b))
	b = []byte{255, 0, 0, 0, 0, 0}
	assert.Equal(t, uint64(255), readUint48(b))
}

func TestGetParentTile(t *testing.T) {
	a := Zxy{Z: 8, X: 125, Y: 69}
	assert.Equal(t, Zxy{Z: 7, X: 62, Y: 34}, getParentTile(a, 7))
}
