package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResolver(t *testing.T) {
	resolver := newResolver(true, true)
	resolver.AddTileIsNew(1, []byte{0x1, 0x2})
	assert.Equal(t, 1, len(resolver.Entries))
	resolver.AddTileIsNew(2, []byte{0x1, 0x3})
	assert.Equal(t, uint64(52), resolver.Offset)
	isNew, _ := resolver.AddTileIsNew(3, []byte{0x1, 0x2})
	assert.False(t, isNew)
	assert.Equal(t, uint64(52), resolver.Offset)
	resolver.AddTileIsNew(4, []byte{0x1, 0x2})
	assert.Equal(t, 3, len(resolver.Entries))
	resolver.AddTileIsNew(6, []byte{0x1, 0x2})
	assert.Equal(t, 4, len(resolver.Entries))
}

func TestV2UpgradeBarebones(t *testing.T) {
	header, jsonMetadata, err := v2ToHeaderJSON(map[string]interface{}{
		"bounds":      "-180.0,-85,178,83",
		"attribution": "abcd",
	}, []byte{0x1f, 0x8b, 0x0, 0x0})
	assert.Nil(t, err)
	_, ok := jsonMetadata["attribution"]
	assert.True(t, ok)
	assert.Equal(t, int32(-180*10000000), header.MinLonE7)
	assert.Equal(t, int32(-85*10000000), header.MinLatE7)
	assert.Equal(t, int32(178*10000000), header.MaxLonE7)
	assert.Equal(t, int32(83*10000000), header.MaxLatE7)
	_, ok = jsonMetadata["bounds"]
	assert.False(t, ok)
	assert.Equal(t, Gzip, int(header.TileCompression))
	assert.Equal(t, Mvt, int(header.TileType))
}

func TestV2UpgradeExtra(t *testing.T) {
	// with the fields tippecanoe usually has
	header, jsonMetadata, err := v2ToHeaderJSON(map[string]interface{}{
		"bounds":      "-180.0,-85,180,85",
		"center":      "-122.1906,37.7599,11",
		"format":      "pbf",
		"compression": "gzip",
		"json":        "{\"abc\":\"def\"}",
	}, []byte{0x0, 0x0, 0x0, 0x0})
	assert.Nil(t, err)
	assert.Equal(t, int32(-122.1906*10000000), header.CenterLonE7)
	assert.Equal(t, int32(37.7599*10000000), header.CenterLatE7)
	assert.Equal(t, uint8(11), header.CenterZoom)
	_, ok := jsonMetadata["center"]
	assert.False(t, ok)
	_, ok = jsonMetadata["abc"]
	assert.True(t, ok)
}

func TestZoomCenterDefaults(t *testing.T) {
	// with no center set
	header := HeaderV3{}
	header.MinLonE7 = -45 * 10000000
	header.MaxLonE7 = -43 * 10000000
	header.MinLatE7 = 21 * 10000000
	header.MaxLatE7 = 23 * 10000000
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{ZxyToID(3, 0, 0), 0, 0, 0})
	entries = append(entries, EntryV3{ZxyToID(4, 0, 0), 1, 1, 1})
	setZoomCenterDefaults(&header, entries)
	assert.Equal(t, uint8(3), header.MinZoom)
	assert.Equal(t, uint8(4), header.MaxZoom)
	assert.Equal(t, uint8(3), header.CenterZoom)
	assert.Equal(t, int32(-44*10000000), header.CenterLonE7)
	assert.Equal(t, int32(22*10000000), header.CenterLatE7)

	// with a center set
	header = HeaderV3{}
	header.MinLonE7 = -45 * 10000000
	header.MaxLonE7 = -43 * 10000000
	header.MinLatE7 = 21 * 10000000
	header.MaxLatE7 = 23 * 10000000
	header.CenterLonE7 = header.MinLonE7
	header.CenterLatE7 = header.MinLatE7
	header.CenterZoom = 4
	setZoomCenterDefaults(&header, entries)
	assert.Equal(t, uint8(4), header.CenterZoom)
	assert.Equal(t, int32(-45*10000000), header.CenterLonE7)
	assert.Equal(t, int32(21*10000000), header.CenterLatE7)
}

func TestV2UpgradeInfer(t *testing.T) {
	header, _, err := v2ToHeaderJSON(map[string]interface{}{
		"bounds": "-180.0,-85,180,85",
	}, []byte{0xff, 0xd8, 0xff, 0xe0})
	assert.Nil(t, err)
	assert.Equal(t, Jpeg, int(header.TileType))
	assert.Equal(t, NoCompression, int(header.TileCompression))
	header, _, err = v2ToHeaderJSON(map[string]interface{}{
		"bounds": "-180.0,-85,180,85",
	}, []byte{0x89, 0x50, 0x4e, 0x47})
	assert.Nil(t, err)
	assert.Equal(t, Png, int(header.TileType))
	assert.Equal(t, NoCompression, int(header.TileCompression))
	header, _, err = v2ToHeaderJSON(map[string]interface{}{
		"bounds": "-180.0,-85,180,85",
	}, []byte{0x00, 00, 00, 00})
	assert.Equal(t, Mvt, int(header.TileType))
	assert.Equal(t, UnknownCompression, header.TileCompression)
	header, _, err = v2ToHeaderJSON(map[string]interface{}{
		"bounds": "-180.0,-85,180,85",
	}, []byte{0x1f, 0x8b, 00, 00})
	assert.Nil(t, err)
	assert.Equal(t, Mvt, int(header.TileType))
	assert.Equal(t, Gzip, int(header.TileCompression))
}

func TestMbtiles(t *testing.T) {
	header, jsonMetadata, err := mbtilesToHeaderJSON([]string{
		"name", "test_name",
		"format", "pbf",
		"bounds", "-180.0,-85,180,85",
		"center", "-122.1906,37.7599,11",
		"attribution", "<div>abc</div>",
		"description", "a description",
		"type", "overlay",
		"version", "1",
		"json", "{\"vector_layers\":[{\"abc\":123}],\"tilestats\":{\"def\":456}}",
		"compression", "gzip",
	})
	assert.Nil(t, err)
	assert.Equal(t, int32(-180*10000000), header.MinLonE7)
	assert.Equal(t, int32(-85*10000000), header.MinLatE7)
	assert.Equal(t, int32(180*10000000), header.MaxLonE7)
	assert.Equal(t, int32(85*10000000), header.MaxLatE7)
	assert.Equal(t, Mvt, int(header.TileType))
	assert.Equal(t, int32(-122.1906*10000000), header.CenterLonE7)
	assert.Equal(t, int32(37.7599*10000000), header.CenterLatE7)
	assert.Equal(t, 11, int(header.CenterZoom))
	assert.Equal(t, Gzip, int(header.TileCompression))

	// assert removal of redundant fields

	_, ok := jsonMetadata["center"]
	assert.False(t, ok)
	_, ok = jsonMetadata["bounds"]
	assert.False(t, ok)

	// assert preservation of metadata fields for roundtrip
	_, ok = jsonMetadata["name"]
	assert.True(t, ok)

	_, ok = jsonMetadata["format"]
	assert.True(t, ok)

	_, ok = jsonMetadata["attribution"]
	assert.True(t, ok)

	_, ok = jsonMetadata["description"]
	assert.True(t, ok)

	_, ok = jsonMetadata["type"]
	assert.True(t, ok)

	_, ok = jsonMetadata["version"]
	assert.True(t, ok)

	_, ok = jsonMetadata["compression"]
	assert.True(t, ok)

	// assert well-known json fields at top level
	_, ok = jsonMetadata["vector_layers"]
	assert.True(t, ok)

	_, ok = jsonMetadata["tilestats"]
	assert.True(t, ok)
}

func TestMbtilesMissingBoundsCenter(t *testing.T) {
	header, _, err := mbtilesToHeaderJSON([]string{
		"name", "test_name",
		"format", "pbf",
		"attribution", "<div>abc</div>",
		"description", "a description",
		"type", "overlay",
		"version", "1",
		"json", "{\"vector_layers\":[{\"abc\":123}],\"tilestats\":{\"def\":456}}",
		"compression", "gzip",
	})
	assert.Nil(t, err)
	assert.Equal(t, int32(-180*10000000), header.MinLonE7)
	assert.Equal(t, int32(-85*10000000), header.MinLatE7)
	assert.Equal(t, int32(180*10000000), header.MaxLonE7)
	assert.Equal(t, int32(85*10000000), header.MaxLatE7)
	assert.Equal(t, int32(0), header.CenterLonE7)
	assert.Equal(t, int32(0), header.CenterLatE7)
}

func TestMbtilesDegenerateBounds(t *testing.T) {
	_, _, err := mbtilesToHeaderJSON([]string{
		"name", "test_name",
		"format", "pbf",
		"bounds", "0,0,0,0",
		"attribution", "<div>abc</div>",
		"description", "a description",
		"type", "overlay",
		"version", "1",
		"json", "{\"vector_layers\":[{\"abc\":123}],\"tilestats\":{\"def\":456}}",
		"compression", "gzip",
	})
	assert.NotNil(t, err)
}
