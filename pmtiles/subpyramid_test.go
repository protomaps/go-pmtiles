package pmtiles

import (
	"testing"
)

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
