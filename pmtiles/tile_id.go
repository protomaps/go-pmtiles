package pmtiles

import (
	"math/bits"
)

func rotate(n uint32, x uint32, y uint32, rx uint32, ry uint32) (uint32, uint32) {
	if ry == 0 {
		if rx != 0 {
			x = n - 1 - x
			y = n - 1 - y
		}
		return y, x
	}
	return x, y
}

// ZxyToID converts (Z,X,Y) tile coordinates to a Hilbert TileID.
func ZxyToID(z uint8, x uint32, y uint32) uint64 {
	var acc uint64 = (1<<(z*2) - 1) / 3
	n := uint32(z - 1)
	for s := uint32(1 << n); s > 0; s >>= 1 {
		var rx = s & x
		var ry = s & y
		acc += uint64((3*rx)^ry) << n
		x, y = rotate(s, x, y, rx, ry)
		n--
	}
	return acc
}

// IDToZxy converts a Hilbert TileID to (Z,X,Y) tile coordinates.
func IDToZxy(i uint64) (uint8, uint32, uint32) {
	var z = uint8(bits.Len64(3*i+1)-1) / 2
	var acc = (uint64(1)<<(z*2) - 1) / 3
	var t = i - acc
	var tx, ty uint32
	for a := uint8(0); a < z; a++ {
		var s = uint32(1) << a
		var rx = 1 & (uint32(t) >> 1)
		var ry = 1 & (uint32(t) ^ rx)
		tx, ty = rotate(s, tx, ty, rx, ry)
		tx += rx << a
		ty += ry << a
		t >>= 2
	}
	return uint8(z), tx, ty
}

// ParentID efficiently finds a parent Hilbert TileID without converting to (Z,X,Y).
func ParentID(i uint64) uint64 {
	var z = uint8(64-bits.LeadingZeros64(3*i+1)-1) / 2
	var acc uint64 = (1<<(z*2) - 1) / 3
	var parentAcc uint64 = (1<<((z-1)*2) - 1) / 3
	return parentAcc + (i-acc)/4
}
