package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"encoding/binary"
	"time"
)

func TestDownloadParts(t *testing.T) {
	fakeGet := func(rng Range) []byte {
		time.Sleep(time.Millisecond * time.Duration(3 - rng.Offset))
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, rng.Offset)
		return bytes
	}

	wanted := make([]Range, 0)
	wanted = append(wanted, Range{0,1})
	wanted = append(wanted, Range{1,2})
	wanted = append(wanted, Range{2,3})

	result := DownloadParts(fakeGet, wanted, 3)

	expected := uint64(0)

	for x := range result {
		assert.Equal(t, expected, binary.LittleEndian.Uint64(x))
		expected += 1
	}

	assert.Equal(t, expected, uint64(3))
}
