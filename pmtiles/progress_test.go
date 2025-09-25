package pmtiles

import (
	"bytes"
	"sync"
	"testing"
)

// Mock progress writer for testing
type mockProgressWriter struct {
	countProgressCalls []mockProgressCall
	bytesProgressCalls []mockProgressCall
	mu                 sync.Mutex
}

type mockProgressCall struct {
	total       int64
	description string
}

func (m *mockProgressWriter) NewCountProgress(total int64, description string) Progress {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.countProgressCalls = append(m.countProgressCalls, mockProgressCall{total, description})
	return &mockProgress{total: total, description: description}
}

func (m *mockProgressWriter) NewBytesProgress(total int64, description string) Progress {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytesProgressCalls = append(m.bytesProgressCalls, mockProgressCall{total, description})
	return &mockProgress{total: total, description: description}
}

func (m *mockProgressWriter) getCountProgressCalls() []mockProgressCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]mockProgressCall{}, m.countProgressCalls...)
}

func (m *mockProgressWriter) getBytesProgressCalls() []mockProgressCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]mockProgressCall{}, m.bytesProgressCalls...)
}

func (m *mockProgressWriter) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.countProgressCalls = nil
	m.bytesProgressCalls = nil
}

// Mock progress implementation
type mockProgress struct {
	total       int64
	current     int64
	description string
	closed      bool
	addCalls    []int
	writeCalls  [][]byte
	mu          sync.Mutex
}

func (p *mockProgress) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writeCalls = append(p.writeCalls, append([]byte{}, data...))
	p.current += int64(len(data))
	return len(data), nil
}

func (p *mockProgress) Add(num int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addCalls = append(p.addCalls, num)
	p.current += int64(num)
}

func (p *mockProgress) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *mockProgress) getCurrent() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *mockProgress) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *mockProgress) getAddCalls() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int{}, p.addCalls...)
}

func (p *mockProgress) getWriteCalls() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([][]byte, len(p.writeCalls))
	for i, call := range p.writeCalls {
		result[i] = append([]byte{}, call...)
	}
	return result
}

// Helper function to reset progress writer state
func resetProgressWriter() {
	progressWriterMu.Lock()
	defer progressWriterMu.Unlock()
	progressWriter = &defaultProgressWriter{}
	quietMode = false
}

func TestSetProgressWriter(t *testing.T) {
	defer resetProgressWriter()

	mock := &mockProgressWriter{}
	SetProgressWriter(mock)

	writer := getProgressWriter()
	if writer != mock {
		t.Errorf("Expected progress writer to be mock, got %T", writer)
	}
}

func TestSetProgressWriterNil(t *testing.T) {
	defer resetProgressWriter()

	SetProgressWriter(nil)

	writer := getProgressWriter()
	if _, ok := writer.(*quietProgressWriter); !ok {
		t.Errorf("Expected progress writer to be quietProgressWriter when set to nil, got %T", writer)
	}
}

func TestSetQuietMode(t *testing.T) {
	defer resetProgressWriter()

	// Test enabling quiet mode
	SetQuietMode(true)
	if !IsQuietMode() {
		t.Error("Expected IsQuietMode() to return true")
	}
	writer := getProgressWriter()
	if _, ok := writer.(*quietProgressWriter); !ok {
		t.Errorf("Expected progress writer to be quietProgressWriter in quiet mode, got %T", writer)
	}

	// Test disabling quiet mode
	SetQuietMode(false)
	if IsQuietMode() {
		t.Error("Expected IsQuietMode() to return false")
	}
	writer = getProgressWriter()
	if _, ok := writer.(*defaultProgressWriter); !ok {
		t.Errorf("Expected progress writer to be defaultProgressWriter when quiet mode disabled, got %T", writer)
	}
}

func TestDefaultProgressWriter(t *testing.T) {
	defer resetProgressWriter()

	// Test with quiet mode disabled
	SetQuietMode(false)
	writer := &defaultProgressWriter{}

	// Test NewCountProgress
	progress := writer.NewCountProgress(100, "test count")
	if progress == nil {
		t.Error("Expected non-nil progress from NewCountProgress")
	}

	// Test NewBytesProgress
	progress = writer.NewBytesProgress(1024, "test bytes")
	if progress == nil {
		t.Error("Expected non-nil progress from NewBytesProgress")
	}
}

func TestDefaultProgressWriterQuietMode(t *testing.T) {
	defer resetProgressWriter()

	// Test with quiet mode enabled
	SetQuietMode(true)
	writer := &defaultProgressWriter{}

	// Test NewCountProgress in quiet mode
	progress := writer.NewCountProgress(100, "test count")
	if _, ok := progress.(*quietProgress); !ok {
		t.Errorf("Expected quietProgress in quiet mode, got %T", progress)
	}

	// Test NewBytesProgress in quiet mode
	progress = writer.NewBytesProgress(1024, "test bytes")
	if _, ok := progress.(*quietProgress); !ok {
		t.Errorf("Expected quietProgress in quiet mode, got %T", progress)
	}
}

func TestProgressBarWrapper(t *testing.T) {
	defer resetProgressWriter()

	SetQuietMode(false)
	writer := &defaultProgressWriter{}
	progress := writer.NewCountProgress(100, "test")

	// Test Write
	data := []byte("test data")
	n, err := progress.Write(data)
	if err != nil {
		t.Errorf("Unexpected error from Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected Write to return %d, got %d", len(data), n)
	}

	// Test Add
	progress.Add(10)

	// Test Close
	if err := progress.Close(); err != nil {
		t.Errorf("Unexpected error from Close: %v", err)
	}
}

func TestProgressBarWrapperNilBar(t *testing.T) {
	wrapper := &progressBarWrapper{bar: nil}

	// Test Write with nil bar
	data := []byte("test")
	n, err := wrapper.Write(data)
	if err != nil {
		t.Errorf("Unexpected error from Write with nil bar: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected Write to return %d with nil bar, got %d", len(data), n)
	}

	// Test Add with nil bar (should not panic)
	wrapper.Add(5)

	// Test Close with nil bar
	if err := wrapper.Close(); err != nil {
		t.Errorf("Unexpected error from Close with nil bar: %v", err)
	}
}

func TestQuietProgressWriter(t *testing.T) {
	writer := &quietProgressWriter{}

	// Test NewCountProgress
	progress := writer.NewCountProgress(100, "test count")
	if _, ok := progress.(*quietProgress); !ok {
		t.Errorf("Expected quietProgress, got %T", progress)
	}

	// Test that Add method is callable on the returned progress (covers the missing line)
	progress.Add(50)

	// Test NewBytesProgress
	progress = writer.NewBytesProgress(1024, "test bytes")
	if _, ok := progress.(*quietProgress); !ok {
		t.Errorf("Expected quietProgress, got %T", progress)
	}

	// Test that Add method is callable on this progress too
	progress.Add(256)
}

func TestQuietProgress(t *testing.T) {
	progress := &quietProgress{}

	// Test Write
	data := []byte("test data")
	n, err := progress.Write(data)
	if err != nil {
		t.Errorf("Unexpected error from quiet Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected quiet Write to return %d, got %d", len(data), n)
	}

	// Test Add (should not panic and be no-op)
	progress.Add(10)
	progress.Add(25)

	// Test Close
	if err := progress.Close(); err != nil {
		t.Errorf("Unexpected error from quiet Close: %v", err)
	}
}

func TestQuietProgressDirect(t *testing.T) {
	// Directly test quietProgress.Add to ensure coverage
	progress := &quietProgress{}
	progress.Add(100) // This should execute the no-op function
}

func TestMockProgressWriter(t *testing.T) {
	defer resetProgressWriter()

	mock := &mockProgressWriter{}
	SetProgressWriter(mock)

	writer := getProgressWriter()

	// Test NewCountProgress
	progress1 := writer.NewCountProgress(100, "count test")
	calls := mock.getCountProgressCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 count progress call, got %d", len(calls))
	}
	if calls[0].total != 100 || calls[0].description != "count test" {
		t.Errorf("Unexpected count progress call: total=%d, description=%s", calls[0].total, calls[0].description)
	}

	// Test NewBytesProgress
	progress2 := writer.NewBytesProgress(1024, "bytes test")
	byteCalls := mock.getBytesProgressCalls()
	if len(byteCalls) != 1 {
		t.Errorf("Expected 1 bytes progress call, got %d", len(byteCalls))
	}
	if byteCalls[0].total != 1024 || byteCalls[0].description != "bytes test" {
		t.Errorf("Unexpected bytes progress call: total=%d, description=%s", byteCalls[0].total, byteCalls[0].description)
	}

	// Test progress operations
	progress1.Add(25)
	progress1.Add(35)
	mockProgress1 := progress1.(*mockProgress)
	addCalls := mockProgress1.getAddCalls()
	if len(addCalls) != 2 || addCalls[0] != 25 || addCalls[1] != 35 {
		t.Errorf("Unexpected Add calls: %v", addCalls)
	}
	if mockProgress1.getCurrent() != 60 {
		t.Errorf("Expected current progress to be 60, got %d", mockProgress1.getCurrent())
	}

	// Test Write operations
	data1 := []byte("hello")
	data2 := []byte("world")
	progress2.Write(data1)
	progress2.Write(data2)
	mockProgress2 := progress2.(*mockProgress)
	writeCalls := mockProgress2.getWriteCalls()
	if len(writeCalls) != 2 {
		t.Errorf("Expected 2 write calls, got %d", len(writeCalls))
	}
	if !bytes.Equal(writeCalls[0], data1) || !bytes.Equal(writeCalls[1], data2) {
		t.Errorf("Unexpected write calls: %v", writeCalls)
	}
	if mockProgress2.getCurrent() != 10 {
		t.Errorf("Expected current progress to be 10, got %d", mockProgress2.getCurrent())
	}

	// Test Close
	progress1.Close()
	progress2.Close()
	if !mockProgress1.isClosed() || !mockProgress2.isClosed() {
		t.Error("Expected both progress instances to be closed")
	}
}

func TestConcurrentAccess(t *testing.T) {
	defer resetProgressWriter()

	mock := &mockProgressWriter{}

	var wg sync.WaitGroup
	numRoutines := 10

	// Test concurrent SetProgressWriter calls
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			SetProgressWriter(mock)
			_ = getProgressWriter()
		}()
	}

	wg.Wait()

	// Test concurrent progress creation
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			writer := getProgressWriter()
			progress := writer.NewCountProgress(int64(idx*10), "concurrent test")
			progress.Add(1)
			progress.Close()
		}(i)
	}

	wg.Wait()

	calls := mock.getCountProgressCalls()
	if len(calls) != numRoutines {
		t.Errorf("Expected %d concurrent progress calls, got %d", numRoutines, len(calls))
	}
}

func TestSetQuietModeAndCustomWriter(t *testing.T) {
	defer resetProgressWriter()

	mock := &mockProgressWriter{}

	// Set custom writer first
	SetProgressWriter(mock)
	writer1 := getProgressWriter()
	if writer1 != mock {
		t.Error("Expected custom writer to be set")
	}

	// SetQuietMode should override custom writer
	SetQuietMode(true)
	writer2 := getProgressWriter()
	if _, ok := writer2.(*quietProgressWriter); !ok {
		t.Errorf("Expected quiet writer after SetQuietMode(true), got %T", writer2)
	}
	if !IsQuietMode() {
		t.Error("Expected IsQuietMode() to return true")
	}

	// SetQuietMode(false) should restore default writer, not custom
	SetQuietMode(false)
	writer3 := getProgressWriter()
	if _, ok := writer3.(*defaultProgressWriter); !ok {
		t.Errorf("Expected default writer after SetQuietMode(false), got %T", writer3)
	}
	if IsQuietMode() {
		t.Error("Expected IsQuietMode() to return false")
	}

	// Setting custom writer again should work
	SetProgressWriter(mock)
	writer4 := getProgressWriter()
	if writer4 != mock {
		t.Error("Expected custom writer to be set again")
	}
}