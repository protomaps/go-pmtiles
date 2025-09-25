package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/protomaps/go-pmtiles/pmtiles"
)

// CloudProgress represents a custom progress writer
type CloudProgress struct {
	serviceName string
}

// CustomProgress tracks progress for individual operations
type CustomProgress struct {
	serviceName string
	operation   string
	total       int64
	current     int64
	startTime   time.Time
}

// NewCountProgress creates a progress tracker for count-based operations
func (c *CloudProgress) NewCountProgress(total int64, description string) pmtiles.Progress {
	fmt.Printf("[%s] Starting operation: %s (total items: %d)\n", c.serviceName, description, total)
	return &CustomProgress{
		serviceName: c.serviceName,
		operation:   description,
		total:       total,
		current:     0,
		startTime:   time.Now(),
	}
}

// NewBytesProgress creates a progress tracker for byte-based operations
func (c *CloudProgress) NewBytesProgress(total int64, description string) pmtiles.Progress {
	fmt.Printf("[%s] Starting operation: %s (total bytes: %s)\n",
		c.serviceName, description, formatBytes(total))
	return &CustomProgress{
		serviceName: c.serviceName,
		operation:   description,
		total:       total,
		current:     0,
		startTime:   time.Now(),
	}
}

// Write implements io.Writer for byte-based progress tracking
func (p *CustomProgress) Write(data []byte) (int, error) {
	p.current += int64(len(data))
	p.reportProgress()

	// Here you would typically send the progress update to some external service

	return len(data), nil
}

// Add increments the progress counter for count-based operations
func (p *CustomProgress) Add(num int) {
	p.current += int64(num)
	p.reportProgress()

	// Send progress update to outside service here if needed
}

// Close finalizes the progress tracking
func (p *CustomProgress) Close() error {
	duration := time.Since(p.startTime)
	fmt.Printf("[%s] Completed: %s in %v\n", p.serviceName, p.operation, duration.Round(time.Millisecond))

	// Send completion notification to outside service here if needed
	return nil
}

// reportProgress displays current progress and could send updates to cloud services
func (p *CustomProgress) reportProgress() {
	if p.total <= 0 {
		return
	}

	percentage := float64(p.current) / float64(p.total) * 100
	elapsed := time.Since(p.startTime)

	// Create a simple progress bar
	barWidth := 30
	filled := int(percentage / 100 * float64(barWidth))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	if strings.Contains(p.operation, "bytes") || p.total > 1000 {
		// Byte-based progress
		fmt.Printf("[%s] %s: %.1f%% [%s] %s/%s (%.2fs)\n",
			p.serviceName, p.operation, percentage, bar,
			formatBytes(p.current), formatBytes(p.total), elapsed.Seconds())
	} else {
		// Count-based progress
		fmt.Printf("[%s] %s: %.1f%% [%s] %d/%d items (%.2fs)\n",
			p.serviceName, p.operation, percentage, bar,
			p.current, p.total, elapsed.Seconds())
	}
}

// formatBytes converts bytes to human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func main() {
	// Custom progress writer for external services
	fmt.Println("\nCustom Cloud Progress Writer:")
	cloudProgress := &CloudProgress{serviceName: "Service"}
	pmtiles.SetProgressWriter(cloudProgress)

	// Simulate some progress operations
	fmt.Println("\nSimulating PMTiles operations with custom progress reporting:")

	// Simulate a count-based operation
	progressCount := cloudProgress.NewCountProgress(100, "Processing tiles")
	for i := 0; i < 100; i += 10 {
		progressCount.Add(10)
		time.Sleep(50 * time.Millisecond) // Simulate work
	}
	progressCount.Close()

	// Simulate a bytes-based operation
	progressBytes := cloudProgress.NewBytesProgress(1024*1024, "Uploading tiles")
	data := make([]byte, 64*1024) // 64KB chunks
	for i := 0; i < 16; i++ {
		progressBytes.Write(data)
		time.Sleep(30 * time.Millisecond) // Simulate upload time
	}
	progressBytes.Close()
}
