package ingest

import "heaplog_2024/internal/common"

// Local db for files, indexed segments and messages
type filesIndex interface {
	// getSegments returns indexed segments (sorted by position) per file
	getSegments() (map[string][]common.Location, error)
	// terms are data for the inverted index
	putSegment(file string, segment Segment, terms []string, messages []Message) error
	// wipeFile removes all data for a file from the index
	wipeFile(file string) error
}
