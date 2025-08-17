package ingest

import "heaplog_2024/internal/common"

type FilesIndex interface {
	// GetSegments returns indexed segments (sorted by position) per file
	GetSegments() (map[string][]common.Location, error)
	// PutSegment adds a segment to the index
	PutSegment(file string, terms [][]byte, messages []common.Message) (int, error)
	// WipeSegment resets the index for the single segment
	WipeSegment(file string, segment common.Location) error
	// WipeSegments resets the index for the file
	WipeSegments(file string) error
	// WipeFile deletes the index for the file
	WipeFile(file string) error
}
