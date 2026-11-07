package pmtiles

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ResumeOptions turns on resume for ExtractWithResume.
// A resumable extract records its progress in a resume state file.
// A later run with the same source and arguments continues from the recorded progress.
type ResumeOptions struct {
	// StatePath is the path of the resume state file. An empty path turns resume off.
	StatePath string
	// RestartOnUnresumable deletes an output that cannot be resumed, and its resume state file,
	// and starts the extract again. Without it, an output that cannot be resumed is an error.
	RestartOnUnresumable bool
	// KeepState keeps the resume state file, marked complete, after the extract succeeds.
	// Without it, the extract deletes the resume state file when it succeeds.
	KeepState bool
}

// extractResumeStateFormatVersion changes when the resume state file changes its meaning.
// A resume state file of another version cannot be resumed.
const extractResumeStateFormatVersion = 1

// extractResumeCheckpointInterval is the time between two checkpoints of a resumable extract.
var extractResumeCheckpointInterval = time.Minute

// extractResumeState is the content of a resume state file.
// The fields before RangeBytesDone identify the extract. A resume needs all of them to be equal.
type extractResumeState struct {
	FormatVersion int     `json:"format_version"`
	SourceURL     string  `json:"source_url"`
	SourceEtag    string  `json:"source_etag"`
	MinZoom       int8    `json:"min_zoom"`
	MaxZoom       int8    `json:"max_zoom"`
	Bbox          string  `json:"bbox"`
	RegionSHA256  string  `json:"region_sha256"`
	Overfetch     float32 `json:"overfetch"`
	// PlanSHA256 is the SHA-256 of the output's header, directories and download ranges.
	// It is different when another version of pmtiles plans the same extract differently.
	PlanSHA256 string `json:"plan_sha256"`
	// RangeBytesDone holds, for each download range by its source offset, the number of source
	// bytes from the start of the range that are on disk in the output.
	RangeBytesDone map[uint64]uint64 `json:"range_bytes_done"`
	// Complete is true when the output is complete, with its header on disk.
	Complete bool `json:"complete"`
}

func (s extractResumeState) sameExtract(other extractResumeState) bool {
	return s.FormatVersion == other.FormatVersion &&
		s.SourceURL == other.SourceURL &&
		s.SourceEtag == other.SourceEtag &&
		s.MinZoom == other.MinZoom &&
		s.MaxZoom == other.MaxZoom &&
		s.Bbox == other.Bbox &&
		s.RegionSHA256 == other.RegionSHA256 &&
		s.Overfetch == other.Overfetch &&
		s.PlanSHA256 == other.PlanSHA256
}

// readExtractResumeState returns nil, and no error, when the file does not exist.
func readExtractResumeState(path string) (*extractResumeState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state extractResumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// writeExtractResumeState writes the state to a temporary file, syncs it,
// renames it over the resume state file, and syncs the directory.
// A crash at any point leaves either the old or the new resume state file.
func writeExtractResumeState(path string, state extractResumeState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	temporaryPath := path + ".tmp"
	file, err := os.Create(temporaryPath)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// regionSHA256 returns the SHA-256 of the region file, or an empty string when there is no region file.
func regionSHA256(regionFile string) (string, error) {
	if regionFile == "" {
		return "", nil
	}
	data, err := os.ReadFile(regionFile)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// extractPlanSHA256 returns the SHA-256 of the output's header, directories and download ranges,
// with the ranges in order of source offset.
func extractPlanSHA256(headerBytes []byte, rootBytes []byte, leavesBytes []byte, ranges *list.List) string {
	sorted := make([]overfetchRange, 0, ranges.Len())
	for element := ranges.Front(); element != nil; element = element.Next() {
		sorted = append(sorted, element.Value.(overfetchRange))
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Rng.SrcOffset < sorted[j].Rng.SrcOffset })

	hash := sha256.New()
	hash.Write(headerBytes)
	hash.Write(rootBytes)
	hash.Write(leavesBytes)
	number := make([]byte, 8)
	writeNumber := func(n uint64) {
		binary.LittleEndian.PutUint64(number, n)
		hash.Write(number)
	}
	for _, r := range sorted {
		writeNumber(r.Rng.SrcOffset)
		writeNumber(r.Rng.DstOffset)
		writeNumber(r.Rng.Length)
		for _, cd := range r.CopyDiscards {
			writeNumber(cd.Wanted)
			writeNumber(cd.Discard)
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// prepareExtractResume decides how a resumable extract starts.
// It returns the state to continue from, and whether the output is already complete.
// A state with an empty RangeBytesDone and resuming false means a fresh extract.
func prepareExtractResume(options ResumeOptions, output string, identity extractResumeState, outputSize int64) (state extractResumeState, resuming bool, complete bool, err error) {
	saved, readErr := readExtractResumeState(options.StatePath)
	outputInfo, statErr := os.Stat(output)
	outputExists := statErr == nil

	reason := ""
	switch {
	case readErr != nil:
		reason = fmt.Sprintf("the resume state file cannot be read: %v", readErr)
	case saved == nil && outputExists:
		reason = "the output exists, but the resume state file does not"
	case saved == nil:
		// A fresh extract.
	case saved.SourceEtag == "":
		reason = "the source has no ETag, so a change to it cannot be detected"
	case !saved.sameExtract(identity):
		reason = "the resume state file is for a different source, source version, set of arguments or version of pmtiles"
	case !outputExists:
		reason = "the resume state file exists, but the output does not"
	case outputInfo.Size() != outputSize:
		reason = fmt.Sprintf("the output is %d bytes, not the expected %d", outputInfo.Size(), outputSize)
	case saved.Complete:
		return *saved, false, true, nil
	default:
		if saved.RangeBytesDone == nil {
			saved.RangeBytesDone = make(map[uint64]uint64)
		}
		return *saved, true, false, nil
	}

	if reason != "" {
		if !options.RestartOnUnresumable {
			return identity, false, false, fmt.Errorf("cannot resume %s: %s. Pass --restart-on-unresumable to delete it and start again", output, reason)
		}
		for _, path := range []string{output, options.StatePath} {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return identity, false, false, err
			}
		}
	}

	state = identity
	state.RangeBytesDone = make(map[uint64]uint64)
	return state, false, false, nil
}
