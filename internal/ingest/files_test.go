package ingest

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestDiscoverAt(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "test-discover-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files
	files := map[string][]byte{
		filepath.Join(tempDir, "file1.txt"):          []byte("content1"),
		filepath.Join(tempDir, "file2.txt"):          []byte("content2"),
		filepath.Join(tempDir, "test.log"):           []byte("log content"),
		filepath.Join(tempDir, "subdir/file3.txt"):   []byte("content3"),
		filepath.Join(tempDir, "subdir/test.config"): []byte("config"),
	}

	for path, content := range files {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		name     string
		patterns []string
		expected []string
	}{
		{
			name:     "txt files - 1st level",
			patterns: []string{filepath.Join(tempDir, "*.txt")},
			expected: []string{
				filepath.Join(tempDir, "file1.txt"),
				filepath.Join(tempDir, "file2.txt"),
			},
		},
		{
			name: "all txt files",
			patterns: []string{
				filepath.Join(tempDir, "*.txt"),
				filepath.Join(tempDir, "**", "*.txt"),
			},
			expected: []string{
				filepath.Join(tempDir, "file1.txt"),
				filepath.Join(tempDir, "file2.txt"),
				filepath.Join(tempDir, "subdir/file3.txt"),
			},
		},
		{
			name:     "all files in root",
			patterns: []string{filepath.Join(tempDir, "*.*")},
			expected: []string{
				filepath.Join(tempDir, "file1.txt"),
				filepath.Join(tempDir, "file2.txt"),
				filepath.Join(tempDir, "test.log"),
			},
		},
		{
			name: "multiple patterns",
			patterns: []string{
				filepath.Join(tempDir, "*.txt"),
				filepath.Join(tempDir, "*.log"),
			},
			expected: []string{
				filepath.Join(tempDir, "file1.txt"),
				filepath.Join(tempDir, "file2.txt"),
				filepath.Join(tempDir, "test.log"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			discovered, err := DiscoverAt(tc.patterns)
			if err != nil {
				t.Fatal(err)
			}
			if len(discovered) != len(tc.expected) {
				t.Errorf("DiscoverAt() found %d files, want %d", len(discovered), len(tc.expected))
			}
			for path, size := range discovered {
				if !slices.Contains(tc.expected, path) {
					t.Errorf("File %s is unexpected", path)
				}
				if size != len(files[path]) {
					t.Errorf("File %s has size %d, want %d", path, size, len(files[path]))
				}
			}
		})
	}

}
