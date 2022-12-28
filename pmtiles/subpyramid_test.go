package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPointToTile(t *testing.T) {
	assert.Equal(t,Zxy{4, 8, 8},PointToTile(4, 0, 0))
	assert.Equal(t,Zxy{4, 0, 15},PointToTile(4, -180, -85))
	assert.Equal(t,Zxy{4, 0, 0},PointToTile(4, -180, 85))
	assert.Equal(t, Zxy{4, 15, 15},PointToTile(4, 179.999, -85))
	assert.Equal(t, Zxy{4, 15, 0},PointToTile(4, 179.999, 85))
}

func TestMatchSameLevel(t *testing.T) {
	// same level
	assert.True(t, Matches(0, 0, 0, 0, 0, Zxy{0, 0, 0}))
	assert.True(t, Matches(4, 0, 0, 8, 8, Zxy{4, 4, 4}))
}

func TestMatchCandidateLower(t *testing.T) {
	assert.True(t,Matches(4, 0, 0, 8, 8, Zxy{0, 0, 0}))
	assert.True(t,Matches(4, 15, 15, 15, 15, Zxy{0, 0, 0}))
	assert.False(t,Matches(4, 15, 15, 15, 15, Zxy{1, 0, 0}))
	assert.True(t,Matches(4, 15, 15, 15, 15, Zxy{1, 1, 1}))
}

func TestMatchCandidateHigher(t *testing.T) {
	assert.True(t,Matches(4, 0, 0, 8, 8, Zxy{8, 0, 0}))
}
