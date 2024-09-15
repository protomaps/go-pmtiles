package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPartSizeBytes(t *testing.T) {
	assert.Equal(t, 5*1024*1024, partSizeBytes(100))
	assert.Equal(t, 6442451, partSizeBytes(60*1024*1024*1024))
}
