package pmtiles

import (
	"context"
	"errors"
	"testing"
)

func TestGetTilejson(t *testing.T) {
	// Create a mock Bucket implementation for testing
	mockBucket := &MockBucket{}

	// Define the test cases
	testCases := []struct {
		name          string
		bucket        Bucket
		key           string
		tileUrl       string
		expectedData  []byte
		expectedError error
	}{
		{
			name:          "Valid key and tileUrl",
			bucket:        mockBucket,
			key:           "test-key",
			tileUrl:       "https://example.com/tiles/{z}/{x}/{y}.pbf",
			expectedData:  []byte(`{"name": "Test Tileset"}`),
			expectedError: nil,
		},
		{
			name:          "Invalid key",
			bucket:        mockBucket,
			key:           "",
			tileUrl:       "https://example.com/tiles/{z}/{x}/{y}.pbf",
			expectedData:  nil,
			expectedError: errors.New("invalid key"),
		},
		{
			name:          "Invalid tileUrl",
			bucket:        mockBucket,
			key:           "test-key",
			tileUrl:       "",
			expectedData:  nil,
			expectedError: errors.New("invalid tile URL"),
		},
	}

	// Run the test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := GetTilejson(context.Background(), tc.bucket, tc.key, tc.tileUrl)

			// Check the returned data
			if string(data) != string(tc.expectedData) {
				t.Errorf("Expected data: %s, but got: %s", string(tc.expectedData), string(data))
			}

			// Check the returned error
			if (err == nil && tc.expectedError != nil) || (err != nil && tc.expectedError == nil) || (err != nil && tc.expectedError != nil && err.Error() != tc.expectedError.Error()) {
				t.Errorf("Expected error: %v, but got: %v", tc.expectedError, err)
			}
		})
	}
}

// MockBucket is a mock implementation of the Bucket interface for testing
type MockBucket struct{}

func (b *MockBucket) GetObject(ctx context.Context, key string) ([]byte, error) {
	// Implement the mock behavior here
	// Return the expected data based on the key
	if key == "test-key" {
		return []byte(`{"name": "Test Tileset"}`), nil
	}
	return nil, errors.New("object not found")
}
