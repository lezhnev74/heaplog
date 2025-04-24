package ingest

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/lezhnev74/inverted_index_2"

	"heaplog_2024/common"
	"heaplog_2024/db"
)

// SegmentBuffer accommodates messages that are being indexed.
// It is used to buffer messages before they are written to the database.
// Automatically flushes segment messages and allocates a new segment as needed.
type SegmentBuffer struct {
	// terms accumulate all terms from all messages in this segment
	terms       map[string]struct{}
	messagesCnt int
	s           db.Segment
	// measure one segments time to be indexed
	startTime time.Time

	// segmentSize specifies the maximum segment size in bytes.
	segmentSize uint64
	sdb         *db.SegmentsDb
	ii          *inverted_index_2.InvertedIndex
	file        db.File
}

// NewSegmentBuffer creates a new SegmentBuffer.
func NewSegmentBuffer(
	file db.File,
	segmentSize uint64,
	sdb *db.SegmentsDb,
	ii *inverted_index_2.InvertedIndex,
) *SegmentBuffer {
	return &SegmentBuffer{
		terms:       make(map[string]struct{}),
		messagesCnt: 0,
		s:           db.Segment{},
		segmentSize: segmentSize,
		sdb:         sdb,
		ii:          ii,
		file:        file,
	}
}

// Accept accepts a message and adds it to the segment buffer.
// It decides if the b.s is OK for the given message or another one should be picked.
func (b *SegmentBuffer) Accept(m *ScannedTokenizedMessage) (err error, isNew bool) {
	if b.s.Id == 0 {
		// the segment is not selected at this point,
		// try to use an existing segment that adjoins this message.
		b.s, err = b.sdb.SelectSegmentThatAdjoins(b.file.Id, m.From)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("accept message into buffer: %w", err)
			return
		}
	}

	if uint64(b.s.Loc.Len()) > b.segmentSize {
		// overflow
		err = b.flush()
		if err != nil {
			err = fmt.Errorf("flush segment buf: %w", err)
			return
		}
		b.s = db.Segment{} // reset to force it making a new segment
	}

	if b.s.Id == 0 {
		isNew = true
		b.s = db.Segment{}
		b.s.Id, _ = b.sdb.ReserveSegmentId()
		b.s.FileId = b.file.Id
		b.s.Loc = common.Location{From: m.From, To: m.To}
		b.s.DateMin = m.DateTime
		b.s.DateMax = m.DateTime
		b.terms = make(map[string]struct{})
		b.startTime = time.Now()
	}

	b.s.Loc.To = m.To
	b.s.DateMax = m.DateTime
	b.messagesCnt++

	for i := range m.Terms {
		t := unsafe.String(unsafe.SliceData(m.Terms[i]), len(m.Terms[i]))
		b.terms[t] = struct{}{}
	}

	return nil, isNew
}

// Flush saves terms to II and updates cur segment's boundaries in the storage.
func (b *SegmentBuffer) flush() error {

	if b.messagesCnt == 0 {
		return nil
	}

	// Here it has to save data with respect to accidental interruptions.
	// The reserved segment id is used in both messages and II. But in case the interruption happens
	// during the flush, those will point to a non-existing segment.
	// In some cases that can lead to disk space wasted.
	// Saving the segment as the last step gives more guarantees that no empty segments will appear.

	// update II
	segmentTerms := make([][]byte, 0, len(b.terms))
	for i := range b.terms {
		b := unsafe.Slice(unsafe.StringData(i), len(i))
		segmentTerms = append(segmentTerms, b)
	}
	iiErr := b.ii.Put(segmentTerms, b.s.Id)
	if iiErr != nil {
		return fmt.Errorf("save segment buf: ii: %w", iiErr)
	}

	// as the last step, persist the segment
	err := b.sdb.CheckinSegmentWithId(b.s.Id, b.file.Id, b.s.Loc, b.s.DateMin, b.s.DateMax)
	if err != nil {
		return fmt.Errorf("sync segment: %w", err)
	}

	// Report
	common.Out(
		"indexed %s[%d:%d]: %d messages, %d terms in %s",
		b.file.Path,
		b.s.Loc.From,
		b.s.Loc.To,
		b.messagesCnt,
		len(segmentTerms),
		time.Since(b.startTime).String(),
	)

	// Cleanup
	b.terms = make(map[string]struct{}) // reset for the next segment
	b.messagesCnt = 0
	b.startTime = time.Now()

	return nil
}
