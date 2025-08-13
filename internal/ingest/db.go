package ingest

import "heaplog_2024/internal/common"

// Local db for files, indexed segments and messages
type filesIndex interface {
	// getSegments returns indexed segments (sorted by position) per file
	getSegments() (map[string][]common.Location, error)
	// putSegment adds a segment to the index
	// terms are data for the inverted index
	// must keep invariant that segments are non-overlapping
	putSegment(file string, terms [][]byte, messages []common.Message) error
	// wipeSegments resets the index for the single segment
	wipeSegment(file string, segment common.Location) error
	// wipeSegments resets the index for the file
	wipeSegments(file string) error
	// wipeFile deletes the index for the file
	wipeFile(file string) error
}
