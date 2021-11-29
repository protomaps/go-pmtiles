package pmtiles

import (
	"bytes"
	"testing"
)

func TestWriteUint24(t *testing.T) {
	var i uint32
	i = 16777215
	result := writeUint24(i)
	if !bytes.Equal(result, []byte{255, 255, 255}) {
		t.Errorf("result did not match, was %d", result)
	}
	i = 255
	result = writeUint24(i)
	if !bytes.Equal(result, []byte{255, 0, 0}) {
		t.Errorf("result did not match, was %d", result)
	}
}

func TestWriteUint48(t *testing.T) {
	var i uint64
	i = 281474976710655
	result := writeUint48(i)
	if !bytes.Equal(result, []byte{255, 255, 255, 255, 255, 255}) {
		t.Errorf("result did not match, was %d", result)
	}
	i = 255
	result = writeUint48(i)
	if !bytes.Equal(result, []byte{255, 0, 0, 0, 0, 0}) {
		t.Errorf("result did not match, was %d", result)
	}
}
