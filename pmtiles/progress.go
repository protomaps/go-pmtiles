package pmtiles

import (
	"io"
	"sync"

	"github.com/schollz/progressbar/v3"
)

// ProgressWriter is an interface for progress reporting during pmtiles operations.
// It supports both count-based and byte-based progress tracking.
type ProgressWriter interface {
	// NewCountProgress creates a progress tracker for count-based operations
	NewCountProgress(total int64, description string) Progress
	// NewBytesProgress creates a progress tracker for byte-based operations
	NewBytesProgress(total int64, description string) Progress
}

// Progress represents an active progress tracker that can be updated and written to
type Progress interface {
	io.Writer
	// Add increments the progress by the specified amount
	Add(num int)
	// Close finalizes the progress tracker
	Close() error
}

var (
	// Global progress writer, protected by mutex
	progressWriterMu sync.RWMutex
	progressWriter   ProgressWriter
)

func init() {
	// Initialize with default progress writer
	progressWriter = &defaultProgressWriter{}
}

// SetProgressWriter sets a custom progress writer for all pmtiles operations.
// Pass nil to disable all progress reporting.
func SetProgressWriter(pw ProgressWriter) {
	progressWriterMu.Lock()
	defer progressWriterMu.Unlock()
	if pw == nil {
		progressWriter = &quietProgressWriter{}
	} else {
		progressWriter = pw
	}
}

// getProgressWriter returns the current progress writer
func getProgressWriter() ProgressWriter {
	progressWriterMu.RLock()
	defer progressWriterMu.RUnlock()
	return progressWriter
}

// SetQuietMode enables or disables quiet mode for all pmtiles operations.
// When quiet mode is enabled, progress bars and verbose logging are suppressed.
// This function is maintained for backward compatibility.
func SetQuietMode(quiet bool) {
	progressWriterMu.Lock()
	defer progressWriterMu.Unlock()
	quietMode = quiet
	if quiet {
		progressWriter = &quietProgressWriter{}
	} else {
		progressWriter = &defaultProgressWriter{}
	}
}

// IsQuietMode returns the current quiet mode setting.
func IsQuietMode() bool {
	return quietMode
}

// defaultProgressWriter implements ProgressWriter using the schollz/progressbar library
type defaultProgressWriter struct{}

func (d *defaultProgressWriter) NewCountProgress(total int64, description string) Progress {
	if quietMode {
		return &quietProgress{}
	}
	bar := progressbar.Default(total, description)
	return &progressBarWrapper{bar: bar}
}

func (d *defaultProgressWriter) NewBytesProgress(total int64, description string) Progress {
	if quietMode {
		return &quietProgress{}
	}
	bar := progressbar.DefaultBytes(total, description)
	return &progressBarWrapper{bar: bar}
}

// progressBarWrapper wraps schollz/progressbar to implement our Progress interface
type progressBarWrapper struct {
	bar *progressbar.ProgressBar
}

func (p *progressBarWrapper) Write(data []byte) (int, error) {
	if p.bar == nil {
		return len(data), nil
	}
	return p.bar.Write(data)
}

func (p *progressBarWrapper) Add(num int) {
	if p.bar != nil {
		p.bar.Add(num)
	}
}

func (p *progressBarWrapper) Close() error {
	if p.bar != nil {
		return p.bar.Close()
	}
	return nil
}

// quietProgressWriter implements ProgressWriter with no-op operations
type quietProgressWriter struct{}

func (q *quietProgressWriter) NewCountProgress(total int64, description string) Progress {
	return &quietProgress{}
}

func (q *quietProgressWriter) NewBytesProgress(total int64, description string) Progress {
	return &quietProgress{}
}

// quietProgress is a no-op implementation of Progress
type quietProgress struct{}

func (q *quietProgress) Write(data []byte) (int, error) {
	return len(data), nil
}

func (q *quietProgress) Add(num int) {
	// no-op - intentionally empty
	_ = num // suppress unused parameter warning
}

func (q *quietProgress) Close() error {
	return nil
}
