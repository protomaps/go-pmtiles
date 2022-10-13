package pmtiles

import (
	"testing"
)

func TestPointToTile(t *testing.T) {
	result := PointToTile(4, 0, 0)
	if (result != Zxy{4, 8, 8}) {
		t.Errorf("result did not match, was %d", result)
	}
	result = PointToTile(4, -180, -85)
	if (result != Zxy{4, 0, 15}) {
		t.Errorf("result did not match, was %d", result)
	}
	result = PointToTile(4, -180, 85)
	if (result != Zxy{4, 0, 0}) {
		t.Errorf("result did not match, was %d", result)
	}
	result = PointToTile(4, 179.999, -85)
	if (result != Zxy{4, 15, 15}) {
		t.Errorf("result did not match, was %d", result)
	}
	result = PointToTile(4, 179.999, 85)
	if (result != Zxy{4, 15, 0}) {
		t.Errorf("result did not match, was %d", result)
	}
}

func TestMatchSameLevel(t *testing.T) {
	// same level
	result := Matches(0, 0, 0, 0, 0, Zxy{0, 0, 0})
	if !result {
		t.Errorf("result did not match")
	}
	result = Matches(4, 0, 0, 8, 8, Zxy{4, 4, 4})
	if !result {
		t.Errorf("result did not match")
	}
}

func TestMatchCandidateLower(t *testing.T) {
	result := Matches(4, 0, 0, 8, 8, Zxy{0, 0, 0})
	if !result {
		t.Errorf("result did not match")
	}
	result = Matches(4, 15, 15, 15, 15, Zxy{0, 0, 0})
	if !result {
		t.Errorf("result did not match")
	}
	result = Matches(4, 15, 15, 15, 15, Zxy{1, 0, 0})
	if result {
		t.Errorf("result did not match")
	}
	result = Matches(4, 15, 15, 15, 15, Zxy{1, 1, 1})
	if !result {
		t.Errorf("result did not match")
	}
}

func TestMatchCandidateHigher(t *testing.T) {
	result := Matches(4, 0, 0, 8, 8, Zxy{8, 0, 0})
	if !result {
		t.Errorf("result did not match")
	}
}
