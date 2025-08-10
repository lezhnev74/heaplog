package ingest

import (
	"os"
	"path/filepath"

	"heaplog_2024/internal/common"
)

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
func discoverFilesAt(globs []string) (map[string]common.ErrVal[int], error) {
	files := make(map[string]common.ErrVal[int])
	for _, pattern := range globs {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, path := range matches {
			info, err := os.Stat(path)
			if err != nil {
				files[path] = common.NewErrValE[int](err) // inaccessible directory entry
				continue
			}
			if !info.IsDir() {
				files[path] = common.NewErrValV(int(info.Size()))
			}
		}
	}
	return files, nil
}

const FileOpRemove = "remove"     // FileOpRemove indicates that a file has been removed and its data should be wiped from the index.
const FileOpReindex = "reindex"   // FileOpReindex indicates that a file has been modified unexpectedly and its data should be reindexed.
const FileOpContinue = "continue" // FileOpContinue indicates that a file has been growing correctly and its new data should be indexed normally.
