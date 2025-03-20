package pmtiles

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestCluster(t *testing.T) {
	fileToCluster := makeFixtureCopy(t, "unclustered", "cluster")

	err := Cluster(logger, fileToCluster, true)
	assert.Nil(t, err)

	file, err := os.OpenFile(fileToCluster, os.O_RDONLY, 0666)
	defer file.Close()
	assert.Nil(t, err)

	buf := make([]byte, 127)
	_, _ = file.Read(buf)
	header, _ := DeserializeHeader(buf)
	assert.True(t, header.Clustered)
}
