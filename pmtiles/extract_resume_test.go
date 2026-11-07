package pmtiles

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// resumeTestArchive is a clustered archive of zoom 0 to 4, with leaf directories.
// Each tile holds 2 KiB of pseudo-random bytes, so a cut response ends in the tile data.
func resumeTestArchive() []byte {
	random := rand.New(rand.NewSource(1))
	tiles := make(map[Zxy][]byte)
	for z := uint8(0); z <= 4; z++ {
		for x := uint32(0); x < 1<<z; x++ {
			for y := uint32(0); y < 1<<z; y++ {
				content := make([]byte, 2048)
				random.Read(content)
				tiles[Zxy{z, x, y}] = content
			}
		}
	}
	return fakeArchive(extractTestHeader(), map[string]interface{}{}, tiles, true, Gzip)
}

// resumeTestExtract extracts zoom 0 to 3 of the north-east quarter of the world, so that the
// download has several ranges with gaps between them.
type resumeTestExtract struct {
	url       string
	output    string
	threads   int
	overfetch float32
	maxzoom   int8
	resume    ResumeOptions
}

func (e resumeTestExtract) run() error {
	maxzoom := e.maxzoom
	if maxzoom == 0 {
		maxzoom = 3
	}
	return ExtractWithResume(context.Background(), log.New(io.Discard, "", 0), "", e.url, -1, maxzoom, "", "0,0,179,84", e.output, e.threads, e.overfetch, false, e.resume)
}

// uninterruptedOutput returns the output of the same extract with no interruption and no resume.
func uninterruptedOutput(t *testing.T, archive []byte, extract resumeTestExtract) []byte {
	server := httptest.NewServer(&archiveServer{archive: archive, etag: `"original"`})
	defer server.Close()
	extract.url = server.URL + "/input.pmtiles"
	extract.output = filepath.Join(t.TempDir(), "uninterrupted.pmtiles")
	extract.resume = ResumeOptions{}
	assert.Nil(t, extract.run())
	data, err := os.ReadFile(extract.output)
	assert.Nil(t, err)
	return data
}

func TestExtractResumesAfterInterruptions(t *testing.T) {
	archive := resumeTestArchive()
	for _, threads := range []int{1, 4} {
		for _, overfetch := range []float32{0, 0.5} {
			t.Run(fmt.Sprintf("threads=%d overfetch=%v", threads, overfetch), func(t *testing.T) {
				server := &archiveServer{archive: archive, etag: `"original"`, cutAfterBytes: 6000}
				httpServer := httptest.NewServer(server)
				defer httpServer.Close()
				dir := t.TempDir()
				extract := resumeTestExtract{
					url:       httpServer.URL + "/input.pmtiles",
					output:    filepath.Join(dir, "output.pmtiles"),
					threads:   threads,
					overfetch: overfetch,
					resume:    ResumeOptions{StatePath: filepath.Join(dir, "output.pmtiles.resume.json")},
				}

				attempts := 0
				for {
					attempts++
					if err := extract.run(); err == nil {
						break
					}
					if attempts == 100 {
						t.Fatal("the extract did not complete in 100 attempts")
					}
				}

				assert.Greater(t, attempts, 2)
				resumed, err := os.ReadFile(extract.output)
				assert.Nil(t, err)
				assert.Equal(t, uninterruptedOutput(t, archive, extract), resumed)
				assert.Nil(t, Verify(log.New(io.Discard, "", 0), extract.output))
				_, err = os.Stat(extract.resume.StatePath)
				assert.True(t, os.IsNotExist(err), "the resume state file is deleted after success")
			})
		}
	}
}

// interruptedExtract runs one extract that the server cuts, and returns it with its server.
func interruptedExtract(t *testing.T) (resumeTestExtract, *archiveServer, func()) {
	server := &archiveServer{archive: resumeTestArchive(), etag: `"original"`, cutAfterBytes: 6000}
	httpServer := httptest.NewServer(server)
	dir := t.TempDir()
	extract := resumeTestExtract{
		url:       httpServer.URL + "/input.pmtiles",
		output:    filepath.Join(dir, "output.pmtiles"),
		threads:   1,
		overfetch: 0.05,
		resume:    ResumeOptions{StatePath: filepath.Join(dir, "output.pmtiles.resume.json")},
	}
	assert.NotNil(t, extract.run())
	state, err := readExtractResumeState(extract.resume.StatePath)
	assert.Nil(t, err)
	assert.NotNil(t, state)
	server.mu.Lock()
	server.cutAfterBytes = 0
	server.mu.Unlock()
	return extract, server, httpServer.Close
}

func TestExtractResumeRefusesOtherArguments(t *testing.T) {
	extract, _, closeServer := interruptedExtract(t)
	defer closeServer()
	extract.maxzoom = 2
	err := extract.run()
	assert.ErrorContains(t, err, "cannot resume")

	extract.resume.RestartOnUnresumable = true
	assert.Nil(t, extract.run())
	assert.Nil(t, Verify(log.New(io.Discard, "", 0), extract.output))
}

func TestExtractResumeRefusesAChangedSource(t *testing.T) {
	extract, server, closeServer := interruptedExtract(t)
	defer closeServer()
	server.mu.Lock()
	server.etag = `"replaced"`
	server.mu.Unlock()
	err := extract.run()
	assert.ErrorContains(t, err, "cannot resume")

	extract.resume.RestartOnUnresumable = true
	assert.Nil(t, extract.run())
	assert.Nil(t, Verify(log.New(io.Discard, "", 0), extract.output))
}

func TestExtractResumeRefusesAnOutputWithNoState(t *testing.T) {
	extract, _, closeServer := interruptedExtract(t)
	defer closeServer()
	assert.Nil(t, os.Remove(extract.resume.StatePath))
	err := extract.run()
	assert.ErrorContains(t, err, "cannot resume")

	extract.resume.RestartOnUnresumable = true
	assert.Nil(t, extract.run())
	assert.Nil(t, Verify(log.New(io.Discard, "", 0), extract.output))
}

func TestExtractResumeKeepsTheStateOfACompleteOutput(t *testing.T) {
	extract, server, closeServer := interruptedExtract(t)
	defer closeServer()
	extract.resume.KeepState = true
	assert.Nil(t, extract.run())
	state, err := readExtractResumeState(extract.resume.StatePath)
	assert.Nil(t, err)
	assert.True(t, state.Complete)
	complete, err := os.ReadFile(extract.output)
	assert.Nil(t, err)

	// A run on a complete output makes the same requests as a dry run, so it reads no tiles.
	// It changes nothing.
	requests := func(run func() error) int {
		server.mu.Lock()
		before := server.requests
		server.mu.Unlock()
		assert.Nil(t, run())
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.requests - before
	}
	dryRunRequests := requests(func() error {
		return ExtractWithResume(context.Background(), log.New(io.Discard, "", 0), "", extract.url, -1, 3, "", "0,0,179,84", extract.output, 1, extract.overfetch, true, ResumeOptions{})
	})
	assert.Equal(t, dryRunRequests, requests(extract.run))
	after, err := os.ReadFile(extract.output)
	assert.Nil(t, err)
	assert.Equal(t, complete, after)
}
