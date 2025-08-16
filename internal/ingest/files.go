package ingest

import (
	"iter"
	"os"
	"path/filepath"
)

type fileSize struct {
	path string
	size int
}

// discoverFilesAt searches for files that match the given glob patterns and returns a map containing
// file paths and their corresponding sizes in bytes. For each file found, the map value is an
// ErrVal containing either the file size or an error if the file is inaccessible. Directories
// are skipped during processing. The function returns an error if any of the provided glob
// patterns are invalid.
//
// Parameters:
//   - globs: slice of glob patterns to match files against (e.g., "*.txt", "data/*.log")
//
// Returns:
//   - map[string]common.ErrVal[int]: map of file paths to their sizes or access errors
//   - error: returned if any glob pattern is invalid
func discoverFilesAt(globs []string) iter.Seq2[fileSize, error] {
	return func(yield func(fileSize, error) bool) {
		for _, pattern := range globs {
			matches, err := filepath.Glob(pattern)
			if err != nil {
				if !yield(fileSize{path: pattern}, err) {
					return
				}
				continue
			}
			for _, path := range matches {
				info, err := os.Stat(path)
				if err != nil {
					if !yield(fileSize{path: path}, err) {
						return
					}
					continue
				}
				if !info.IsDir() {
					if !yield(fileSize{path: path, size: int(info.Size())}, err) {
						return
					}
				}
			}
		}
	}
}

const FileOpRemove = "remove"     // FileOpRemove indicates that a file has been removed and its data should be wiped from the index.
const FileOpReindex = "reindex"   // FileOpReindex indicates that a file has been modified unexpectedly and its data should be reindexed.
const FileOpContinue = "continue" // FileOpContinue indicates that a file has been growing correctly and its new data should be indexed normally.
