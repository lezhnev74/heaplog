package ingest

import (
	"os"
	"path/filepath"
)

// DiscoverAt finds files matching the provided glob patterns and returns a map of file paths
// to their sizes in bytes. It skips directories and returns an error if any pattern is invalid
// or files are inaccessible.
func DiscoverAt(globs []string) (map[string]int, error) {
	files := make(map[string]int)
	for _, pattern := range globs {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, path := range matches {
			info, err := os.Stat(path)
			if err != nil {
				continue // inaccessible file
			}
			if !info.IsDir() {
				files[path] = int(info.Size())
			}
		}
	}
	return files, nil
}
