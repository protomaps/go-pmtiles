package pmtiles

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSyncBlockRoundtrip(t *testing.T) {
	var blocks []syncBlock
	blocks = append(blocks, syncBlock{Start: 1, Offset: 2, Length: 3, Hash: 4})
	var b bytes.Buffer
	serializeSyncBlocks(&b, blocks)
	list := deserializeSyncBlocks(1, bufio.NewReader(&b))

	assert.Equal(t, 1, len(list))
	assert.Equal(t, uint64(1), list[0].Start)
	assert.Equal(t, uint64(3), list[0].Length)
	assert.Equal(t, uint64(4), list[0].Hash)
}
