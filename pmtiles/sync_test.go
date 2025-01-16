package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMakeMultiRanges(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 60})
	ranges = append(ranges, srcDstRange{200, 200, 50})
	result := makeMultiRanges(ranges, 10, 15)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].str, "10-59,70-129")
	assert.Equal(t, result[1].str, "210-259")
}
