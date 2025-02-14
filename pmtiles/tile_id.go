package pmtiles

import (
	"math/bits"
)

func rotate(n uint64, x *uint64, y *uint64, rx uint64, ry uint64) {
	if ry == 0 {
		if rx == 1 {
			*x = n - 1 - *x
			*y = n - 1 - *y
		}
		*x, *y = *y, *x
	}
}

// ZxyToID converts (Z,X,Y) tile coordinates to a Hilbert TileID.
func ZxyToID(z uint8, x uint32, y uint32) uint64 {
	var acc uint64 = ((1 << (z * 2)) - 1) / 3
	var tx, ty uint64 = uint64(x), uint64(y)
	for a := int32(z - 1); a >= 0; a-- {
		var rx uint64 = (tx >> a) & 1
		var ry uint64 = (ty >> a) & 1
		var s uint64 = (1 << a)
		rotate(s, &tx, &ty, rx, ry)
		acc += s * s * ((3 * rx) ^ ry)
	}
	return acc
}

// IDToZxy converts a Hilbert TileID to (Z,X,Y) tile coordinates.
func IDToZxy(i uint64) (uint8, uint32, uint32) {
	var z uint8 = uint8((64 - bits.LeadingZeros64(3*i+1) - 1) / 2)
	var acc uint64 = (1<<(z*2) - 1) / 3
	var pos uint64 = i - acc
	var tx, ty uint64 = 0, 0
	for a := uint8(0); a < z; a++ {
		var rx uint64 = (pos / 2) & 1
		var ry uint64 = (pos ^ rx) & 1
		var s uint64 = 1 << a
		rotate(s, &tx, &ty, rx, ry)
		tx += s * rx
		ty += s * ry
		pos /= 4
	}
	return z, uint32(tx), uint32(ty)
}

// ParentID efficiently finds a parent Hilbert TileID without converting to (Z,X,Y).
func ParentID(i uint64) uint64 {
	var z uint8 = uint8((64 - bits.LeadingZeros64(3*i+1) - 1) / 2)
	var acc uint64 = (1<<(z*2) - 1) / 3
	var parentAcc uint64 = (1<<((z-1)*2) - 1) / 3
	return parentAcc + (i-acc)/4
}
