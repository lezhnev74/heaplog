// Package ingest implements file ingestion and indexing functionality.
// It discovers, scans and indexes log files according to configured patterns
// and maintains the index state in a database.
package ingest

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sync"

	"go.uber.org/zap"

	"heaplog_2024/internal/common"
)

// Ingestor handles file discovery, scanning and indexing operations.
// It maintains the index state and ensures data consistency between
// files on disk and their indexed representation.
type Ingestor struct {
	// glob patterns to match files to index
	globs []string
	// regular expression to match messages' starting lines
	messageRE *regexp.Regexp
	// max length of a single indexed file layouts
	segmentLen int
	// number of concurrent workers that index segments
	workers int

	db      filesIndex
	logger  *zap.Logger
	indexer *indexer
}

// Run performs the main ingestion workflow
func (i *Ingestor) Run() error {

	i.logger.Debug("starting ingestion")

	// 1. discover current files
	accessibleFiles, err := i.discoverAccessibleFiles()
	if err != nil {
		return fmt.Errorf("discover accessible files: %w", err)
	}

	// 2. Read indexed state
	indexedSegments, err := i.db.getSegments()
	if err != nil {
		return fmt.Errorf("get indexed segments: %w", err)
	}

	// 3. Reconcile missing files (present in index but not on disk)
	err = i.reconcileMissingFiles(indexedSegments, accessibleFiles)
	if err != nil {
		return fmt.Errorf("reconcile missing files: %w", err)
	}

	// 4. Skip files that are already entirely indexed
	accessibleFiles = i.skipEntirelyIndexed(accessibleFiles, indexedSegments)

	// 5. Build message layouts for all found files
	filesLayouts, err := i.buildFilesLayouts(accessibleFiles)
	if err != nil {
		return fmt.Errorf("build file layouts: %w", err)
	}

	// 6. Validate alignment; wipe segments if misaligned
	// as the result it re-indexes files that somehow changed already indexed data.
	err = i.validateOrWipe(indexedSegments, filesLayouts)
	if err != nil {
		return fmt.Errorf("validate misaligned: %w", err)
	}

	// 7. Validate last file segments.
	// If the last layout is not full and does not end at the end of the file,
	// it is considered to be incomplete and needs to be re-indexed.
	err = i.validateLastSegments(indexedSegments, accessibleFiles)
	if err != nil {
		return fmt.Errorf("validate last segments: %w", err)
	}

	// 8. Plan segment for indexing
	pendingSegments := i.planIndexing(indexedSegments, filesLayouts)

	// 9. Perform indexing
	for r := range i.indexer.indexSegments(pendingSegments) {
		err = i.db.putSegment(r.task.file, r.tokens, r.messages)
		if err != nil {
			i.logger.Error("save indexed segment", zap.String("file", r.task.file), zap.Error(err))
			panic(err)
		}
	}

	return nil
}

// validateOrWipe checks alignment of indexed segments with actual file contents and wipes misaligned files from index.
// It compares each indexed layouts's boundaries with message layouts found in files to ensure they exactly match message boundaries.
// If the file has no messages found or any layouts boundaries don'tokenize align with message boundaries, the file is wiped from the index.
func (i *Ingestor) validateOrWipe(
	indexedSegments map[string][]common.Location,
	foundFilesLayouts map[string][]MessageLayout,
) error {
	var err error
indexFileLoop:
	for file, indexedLocs := range indexedSegments {
		if len(indexedLocs) == 0 {
			continue // no point in comparing, no indexed data available
		}

		// map indexedLocs to actual messages in the file
		for _, s := range indexedLocs {
			leftMatched, rightMatched := false, false

			// todo: apply binary search

			for _, m := range foundFilesLayouts[file] {
				if s.From == m.Loc.From {
					leftMatched = true
				}
				if s.To == m.Loc.To {
					rightMatched = true
				}
			}
			if leftMatched && rightMatched {
				continue
			}

			// the indexed layouts is not aligned to messages in the file
			i.logger.Warn(
				"indexed layouts misalignment: re-index required",
				zap.String("file", file),
				zap.Any("layouts", s),
			)
			err = i.db.wipeSegments(file)
			if err != nil {
				return fmt.Errorf("reindex file: %w", err)
			}
			delete(indexedSegments, file)
			continue indexFileLoop
		}
	}
	return nil
}

// buildFilesLayouts scans accessible files to build message layouts.
// Parameters:
//   - accessibleFiles: map of file paths to their sizes
//
// Returns map of file paths to their message layouts and error if scanning fails.
func (i *Ingestor) buildFilesLayouts(accessibleFiles map[string]int) (map[string][]MessageLayout, error) {

	// split files per workers
	files := slices.Collect(maps.Keys(accessibleFiles))
	filesPerWorker := common.ChunksN(files, i.workers)
	layoutsPerFile := make([][]MessageLayout, len(files))

	wg := sync.WaitGroup{}
	wg.Add(i.workers)
	for j := range filesPerWorker {
		go func() {
			defer wg.Done()
			for _, f := range filesPerWorker[j] {
				fileSize := accessibleFiles[f]
				layouts, err := scan(f, fileSize, i.messageRE.String(), nil)
				if err != nil {
					i.logger.Error("scan file", zap.String("file", f), zap.Error(err))
					continue
				}
				layoutsPerFile[slices.Index(files, f)] = layouts
			}
		}()
	}
	wg.Wait()

	// merge layouts per file to a map
	foundFilesLayouts := make(map[string][]MessageLayout)
	for j, layouts := range layoutsPerFile {
		foundFilesLayouts[files[j]] = layouts
	}
	return foundFilesLayouts, nil
}

// reconcileMissingFiles removes indexed files that are no longer accessible from the indexedSegments map and database.
// Parameters:
//   - indexedSegments: map of file paths to their indexed locations
//   - accessibleFiles: map of currently accessible file paths to their sizes
//
// Returns error if database operation fails.
func (i *Ingestor) reconcileMissingFiles(
	indexedSegments map[string][]common.Location,
	accessibleFiles map[string]int,
) error {
	for file := range indexedSegments {
		if _, ok := accessibleFiles[file]; !ok {
			i.logger.Info("removing file from index", zap.String("file", file))
			err := i.db.wipeFile(file)
			if err != nil {
				return fmt.Errorf("wipe indexed file: %w", err)
			}
			delete(indexedSegments, file)
		}
	}
	return nil
}

// discoverAccessibleFiles discovers and validates files matching the configured glob patterns.
// Returns a map where keys are file paths and values are file sizes in bytes.
// Files that are not accessible are logged with a warning and excluded from the result.
// Returns error if the initial file discovery fails.
func (i *Ingestor) discoverAccessibleFiles() (map[string]int, error) {
	foundFiles := map[string]int{}
	foundFilesEV, err := discoverFilesAt(i.globs)
	if err != nil {
		return nil, fmt.Errorf("discover files via globs: %w", err)
	}
	for file, v := range foundFilesEV {
		if v.Err != nil {
			i.logger.Warn("file not accessible", zap.String("file", file), zap.Error(v.Err))
			delete(foundFilesEV, file)
			continue
		}
		foundFiles[file] = v.Val
	}
	return foundFiles, nil
}

// skipEntirelyIndexed removes files that are already entirely indexed from the accessibleFiles map.
// A file is considered entirely indexed if all its content is covered by indexed segments.
func (i *Ingestor) skipEntirelyIndexed(
	accessibleFiles map[string]int,
	indexedSegments map[string][]common.Location,
) map[string]int {
	result := maps.Clone(accessibleFiles)
	for file, size := range accessibleFiles {
		segments := indexedSegments[file]
		if len(segments) == 0 {
			continue
		}

		fileLocation := common.Location{From: 0, To: size}
		unindexed := fileLocation.RemoveAll(segments)
		if len(unindexed) == 0 {
			i.logger.Debug("skipping entirely indexed file", zap.String("file", file))
			delete(result, file)
		}
	}
	return result
}

// validateLastSegments ensures that the last indexed layouts of each file is either complete
// (has full layouts length) or ends at the end of the file. Incomplete segments that don'tokenize
// meet these criteria are removed from both the in-memory map and the database.
func (i *Ingestor) validateLastSegments(
	indexedSegments map[string][]common.Location,
	accessibleFiles map[string]int,
) error {
	for file, segments := range indexedSegments {
		if len(segments) == 0 {
			continue
		}

		lastSegment := segments[len(segments)-1]
		fileSize := accessibleFiles[file]

		// If the last layouts is not full and doesn'tokenize end at the file end
		if lastSegment.To-lastSegment.From < i.segmentLen && lastSegment.To < fileSize {
			i.logger.Debug(
				"removing incomplete last layouts",
				zap.String("file", file),
				zap.Any("layouts", lastSegment),
			)

			// Remove the last layouts from both map and database
			indexedSegments[file] = segments[:len(segments)-1]
			err := i.db.wipeSegment(file, lastSegment)
			if err != nil {
				return fmt.Errorf("remove incomplete layouts: %w", err)
			}
		}
	}
	return nil
}

// For each file return a list of segments that need to be indexed.
func (i *Ingestor) planIndexing(
	indexedSegments map[string][]common.Location,
	existingFilesLayouts map[string][]MessageLayout,
) map[string][][]MessageLayout {

	plan := make(map[string][][]MessageLayout)

	for file, layouts := range existingFilesLayouts {
		filesize := layouts[len(layouts)-1].Loc.To
		fl := common.Location{0, filesize}
		unindexedLocations := fl.RemoveAll(indexedSegments[file])
		segments := segmentLayoutsByLocations(i.segmentLen, unindexedLocations, layouts)
		plan[file] = segments
	}

	return plan
}
