package pmtiles

import (
	"bytes"
	"encoding/base64"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestCluster(t *testing.T) {
	fileToCluster := makeFixtureCopy(t, "unclustered", "cluster")

	err := Cluster(logger, fileToCluster, true)
	assert.Nil(t, err)

	var b bytes.Buffer
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	err = Show(logger, &b, "", fileToCluster, false, false, false, "", true, 1, 0, 0)
	assert.Nil(t, err)

	PINK := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z/D/PwAHAwL/qGeMxAAAAABJRU5ErkJggg=="
	assert.Equal(t, PINK, base64.StdEncoding.EncodeToString(b.Bytes()))
}
