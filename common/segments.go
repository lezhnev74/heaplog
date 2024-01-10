package common

import (
	"time"
)

// The idea behind indexing of source files is simple.
// During the indexing (ingestion) phase we discover all files and split them in somewhat big segments.
// In each segment we detect individual messages and save all that to the db.
// We save segments in the inverted index to save disk space.
//
// Later during the search phase we use inverted index to find relevant segments. And then select relevant messages.
// For each message we evaluate the query expression.
// As the last step we put matched messages to the storage using only ids (quick ingestion via an appender).

// IndexedSegment describes a region of a file with where all the messages were indexed
// it contains offsets of all messages found in the segment and dates of the first/last messages
type IndexedSegment struct {
	DataSource DataSourceHash
	Messages   []IndexedMessage
}

func (is IndexedSegment) Loc() Location {
	l := Location{0, 0}
	if len(is.Messages) > 0 {
		l.Min = is.Messages[0].Loc.Min
		l.Max = is.Messages[len(is.Messages)-1].Loc.Max
	}
	return l
}
func (is IndexedSegment) MinDate() time.Time { return is.Messages[0].Date }
func (is IndexedSegment) MaxDate() time.Time { return is.Messages[len(is.Messages)-1].Date }

type IndexedMessage struct {
	Id     int64 // ony filled when read from the storage
	Loc    Location
	Date   time.Time
	IsTail bool // detect "tail message"
}

type IndexedSegmentInfo struct {
	Id               int64
	DataSource       DataSourceHash
	MinDate, MaxDate time.Time
	From, To         int64
	Messages         int64
}
