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
	// target length of a single indexed segment (segments align at message boundaries)
	segmentLen int
	// number of concurrent workers that index segments
	workers int

	db      filesIndex
	logger  *zap.Logger
	indexer *Indexer
}

func NewIngestor(
	globs []string,
	messageRE *regexp.Regexp,
	segmentLen int,
	workers int,
	db filesIndex,
	logger *zap.Logger,
	indexer *Indexer,
) *Ingestor {
	if workers <= 0 {
		panic(fmt.Sprintf("invalid workers count: %d", workers))
	}
	if segmentLen <= 0 {
		panic(fmt.Sprintf("invalid segment length: %d", segmentLen))
	}
	if len(globs) == 0 {
		panic("no glob patterns provided")
	}

	return &Ingestor{
		globs:      globs,
		messageRE:  messageRE,
		segmentLen: segmentLen,
		workers:    workers,
		db:         db,
		logger:     logger,
		indexer:    indexer,
	}
}

// Run performs the main ingestion workflow
func (i *Ingestor) Run() error {

	i.logger.Debug("ingestion launched")
	defer i.logger.Debug("ingestion completed")

	// 1. discover current files
	files := map[string]int{}
	for fs, err := range discoverFilesAt(i.globs) {
		if err != nil {
			i.logger.Warn("discover file", zap.String("path", fs.path), zap.Error(err))
			continue
		}
		files[fs.path] = fs.size
	}

	// 2. Read the index
	indexedSegments, err := i.db.getSegments()
	if err != nil {
		return fmt.Errorf("get indexed segments: %w", err)
	}

	// 3. Reconcile missing files (present in index but not on disk)
	for file := range indexedSegments {
		if _, ok := files[file]; !ok {
			i.logger.Info("wipe file index", zap.String("file", file))
			err = i.db.wipeFile(file)
			if err != nil {
				return fmt.Errorf("wipe file index: %w", err)
			}
			delete(indexedSegments, file)
		}
	}

	// 4. Skip files that are already entirely indexed
	for file, size := range files {
		if len(indexedSegments[file]) == 0 {
			continue
		}
		loc := common.Location{From: 0, To: size}
		unindexed := loc.RemoveAll(indexedSegments[file])
		if len(unindexed) == 0 {
			delete(files, file)
		}
	}

	// 5. Build messages' layouts for each file
	layouts, err := i.scanFiles(files)
	if err != nil {
		return fmt.Errorf("build file layouts: %w", err)
	}

	// 6. Validate index alignment; wipe segments if misaligned
	// as a result it re-indexes files that somehow changed already indexed data.
	for file := range findMisalignedSegments(indexedSegments, layouts) {
		i.logger.Warn("indexed misalignment: re-index required", zap.String("file", file))
		err = i.db.wipeSegments(file)
		if err != nil {
			return fmt.Errorf("wipe segments for %s: %w", file, err)
		}
		delete(indexedSegments, file)
	}

	// 7. Validate last file segments.
	// If the last layout is not full and does not end at the end of the file,
	// it is considered to be incomplete and needs to be re-indexed.
	for file := range filesWithIncompleteTrailingSegments(i.segmentLen, indexedSegments, files) {
		i.logger.Debug("re-index trailing segment", zap.String("file", file))
		indexedSegments[file] = indexedSegments[file][:len(indexedSegments[file])-1]
	}

	// 8. Plan segments for indexing
	plan := make(map[string][][]MessageLayout)
	for file := range layouts {
		filesize := layouts[file][len(layouts[file])-1].Loc.To
		loc := common.Location{0, filesize}
		unindexedLocations := loc.RemoveAll(indexedSegments[file])
		segments := alignSegmentsByMessageBoundaries(i.segmentLen, unindexedLocations, layouts[file])
		plan[file] = segments
	}

	// 9. Perform indexing
	for r := range i.indexer.indexSegments(plan) {
		err = i.db.putSegment(r.task.file, r.tokens, r.messages)
		if err != nil {
			return fmt.Errorf("put segment for %s: %w", r.task.file, err)
		}
	}

	return nil
}

// scanFiles scans accessible files to build message layouts.
// Returns a map of file paths to their message layouts and error if scanning fails.
func (i *Ingestor) scanFiles(files map[string]int) (map[string][]MessageLayout, error) {

	// split files per workers
	filePaths := slices.Collect(maps.Keys(files))
	filesPerWorker := common.ChunksN(filePaths, i.workers)
	fileLayouts := make([][]MessageLayout, len(filePaths))

	// scanning is cpu intensive (RE-parsing), so run in parallel
	wg := sync.WaitGroup{}
	wg.Add(i.workers)
	for j := range filesPerWorker {
		go func() {
			defer wg.Done()
			for _, f := range filesPerWorker[j] {
				layouts, err := scan(f, files[f], i.messageRE.String(), nil)
				if err != nil {
					i.logger.Error("scan file", zap.String("file", f), zap.Error(err))
					continue
				}
				fileLayouts[slices.Index(filePaths, f)] = layouts
			}
		}()
	}
	wg.Wait()

	// merge layouts per file to a map
	foundFilesLayouts := make(map[string][]MessageLayout)
	for j, layouts := range fileLayouts {
		foundFilesLayouts[filePaths[j]] = layouts
	}
	return foundFilesLayouts, nil
}
