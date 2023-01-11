package pmtiles

func rotate(n uint64, x *uint64, y *uint64, rx uint64, ry uint64) {
	if ry == 0 {
		if rx == 1 {
			*x = n - 1 - *x
			*y = n - 1 - *y
		}
		*x, *y = *y, *x
	}
}

func t_on_level(z uint8, pos uint64) (uint8, uint32, uint32) {
	var n uint64 = 1 << z
	rx, ry, t := pos, pos, pos
	var tx uint64
	var ty uint64
	var s uint64
	for s = 1; s < n; s *= 2 {
		rx = 1 & (t / 2)
		ry = 1 & (t ^ rx)
		rotate(s, &tx, &ty, rx, ry)
		tx += s * rx
		ty += s * ry
		t /= 4
	}
	return uint8(z), uint32(tx), uint32(ty)
}

func ZxyToId(z uint8, x uint32, y uint32) uint64 {
	var acc uint64
	var tz uint8
	for ; tz < z; tz++ {
		acc += (0x1 << tz) * (0x1 << tz)
	}
	var n uint64 = 1 << z
	var rx uint64
	var ry uint64
	var d uint64
	tx := uint64(x)
	ty := uint64(y)
	for s := n / 2; s > 0; s /= 2 {
		if tx&s > 0 {
			rx = 1
		} else {
			rx = 0
		}
		if ty&s > 0 {
			ry = 1
		} else {
			ry = 0
		}
		d += s * s * ((3 * rx) ^ ry)
		rotate(s, &tx, &ty, rx, ry)
	}
	return acc + d
}

func IdToZxy(i uint64) (uint8, uint32, uint32) {
	var acc uint64
	var z uint8
	for {
		var num_tiles uint64
		num_tiles = (1 << z) * (1 << z)
		if acc+num_tiles > i {
			return t_on_level(z, i-acc)
		}
		acc += num_tiles
		z++
	}
}

// fast parent ID calculation without converting to ZXY.
func ParentId(i uint64) uint64 {
	var acc uint64
	var last_acc uint64
	var z uint8
	for {
		var num_tiles uint64
		num_tiles = (1 << z) * (1 << z)
		if acc+num_tiles > i {
			return last_acc + (i-acc)/4
		}
		last_acc = acc
		acc += num_tiles
		z++
	}

}
