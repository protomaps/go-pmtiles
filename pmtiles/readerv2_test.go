package pmtiles

import (
	"testing"
)

func TestUint24(t *testing.T) {
	b := []byte{255, 255, 255}
	result := readUint24(b)
	if result != 16777215 {
		t.Errorf("result did not match, was %d", result)
	}
	b = []byte{255, 0, 0}
	result = readUint24(b)
	if result != 255 {
		t.Errorf("result did not match, was %d", result)
	}
}

func TestUint48(t *testing.T) {
	b := []byte{255, 255, 255, 255, 255, 255}
	result := readUint48(b)
	if result != 281474976710655 {
		t.Errorf("result did not match, was %d", result)
	}
	b = []byte{255, 0, 0, 0, 0, 0}
	result = readUint48(b)
	if result != 255 {
		t.Errorf("result did not match, was %d", result)
	}
}

func TestGetParentTile(t *testing.T) {
	a := Zxy{Z: 8, X: 125, Y: 69}
	result := GetParentTile(a, 7)
	if (result != Zxy{Z: 7, X: 62, Y: 34}) {
		t.Errorf("result did not match, was %d", result)
	}
}
