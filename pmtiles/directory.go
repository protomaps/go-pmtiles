package pmtiles

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
)

type EntryV3 struct {
	TileId    uint64
	Offset    uint64
	Length    uint32
	RunLength uint32
}

func serialize_entries(entries []EntryV3) bytes.Buffer {
	var b bytes.Buffer
	tmp := make([]byte, binary.MaxVarintLen64)
	w, _ := gzip.NewWriterLevel(&b, gzip.DefaultCompression)

	var n int
	n = binary.PutUvarint(tmp, uint64(len(entries)))
	w.Write(tmp[:n])

	lastId := uint64(0)
	for _, entry := range entries {
		n = binary.PutUvarint(tmp, uint64(entry.TileId)-lastId)
		w.Write(tmp[:n])

		lastId = uint64(entry.TileId)
	}

	for _, entry := range entries {
		n := binary.PutUvarint(tmp, uint64(entry.RunLength))
		w.Write(tmp[:n])
	}

	for _, entry := range entries {
		n := binary.PutUvarint(tmp, uint64(entry.Length))
		w.Write(tmp[:n])
	}

	for i, entry := range entries {
		var n int
		if i > 0 && entry.Offset == entries[i-1].Offset+uint64(entries[i-1].Length) {
			n = binary.PutUvarint(tmp, 0)
		} else {
			n = binary.PutUvarint(tmp, uint64(entry.Offset+1)) // add 1 to not conflict with 0
		}
		w.Write(tmp[:n])
	}

	w.Close()
	return b
}

func deserialize_entries(data bytes.Buffer) []EntryV3 {
	entries := make([]EntryV3, 0)

	reader, _ := gzip.NewReader(&data)
	byte_reader := bufio.NewReader(reader)

	num_entries, _ := binary.ReadUvarint(byte_reader)

	last_id := uint64(0)
	for i := uint64(0); i < num_entries; i++ {
		tmp, _ := binary.ReadUvarint(byte_reader)
		entries = append(entries, EntryV3{last_id + tmp, 0, 0, 0})
		last_id = last_id + tmp
	}

	for i := uint64(0); i < num_entries; i++ {
		run_length, _ := binary.ReadUvarint(byte_reader)
		entries[i].RunLength = uint32(run_length)
	}

	for i := uint64(0); i < num_entries; i++ {
		length, _ := binary.ReadUvarint(byte_reader)
		entries[i].Length = uint32(length)
	}

	for i := uint64(0); i < num_entries; i++ {
		tmp, _ := binary.ReadUvarint(byte_reader)
		if i > 0 && tmp == 0 {
			entries[i].Offset = entries[i-1].Offset + uint64(entries[i-1].Length)
		} else {
			entries[i].Offset = tmp - 1
		}
	}

	return entries
}
