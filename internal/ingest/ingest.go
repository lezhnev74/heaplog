package ingest

import (
	"fmt"
	"regexp"

	"go.uber.org/zap"
)

type Ingestor struct {
	// glob patterns to match files to index
	globs []string
	// regular expression to match messages' starting lines
	messageRE *regexp.Regexp
	// max length of a single indexed file segment
	segmentLength int
	// number of concurrent workers that index segments
	workers int

	db filesIndex
	l  *zap.Logger
}

func (i *Ingestor) Run() error {
	// 1. discover current files
	foundFiles, err := discoverFilesAt(i.globs)
	if err != nil {
		return fmt.Errorf("discover files: %w", err)
	}
	for file, v := range foundFiles {
		if v.Err != nil {
			i.l.Warn("file not accessible", zap.String("file", file), zap.Error(v.Err))
			delete(foundFiles, file)
		}
	}

	// 2. read currently indexed segments and files
	indexedSegments, err := i.db.getSegments()
	if err != nil {
		return fmt.Errorf("get indexed segments: %w", err)
	}

	// 3. Remove files that are no longer present
	for file := range indexedSegments {
		if _, ok := foundFiles[file]; !ok {
			i.l.Info("Removing file from index", zap.String("file", file))
			err = i.db.wipeFile(file)
			if err != nil {
				return fmt.Errorf("wipe file: %w", err)
			}
			delete(indexedSegments, file)
		}
	}

	// 4. Remove files whose contents changed unexpectedly.
	// This is done by mapping indexed segments on top of current file's messages layout.
	// If segments map correctly (exactly at messages boundaries), then the file is still valid,
	// and further accumulative indexing should be performed.
	for file := range indexedSegments {

	}

	return nil
}
