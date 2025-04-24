package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"time"

	"github.com/lezhnev74/inverted_index_2"
	"golang.org/x/sync/errgroup"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/scanner"
)

// Ingest finds unindexed file runs and performs the indexing procedure, updating the database.
type Ingest struct {
	fdb *db.FilesDb
	mdb *db.MessagesDb
	sdb *db.SegmentsDb
	ii  *inverted_index_2.InvertedIndex

	// findMessages extracts message layouts (boundaries) from the file
	findMessages func(file string, locations []common.Location) ([]scanner.MessageLayout, error)
	// parseTime scans the date area of a message and extracts Time
	parseTime func([]byte) (time.Time, error)
	// tokenize converts a message into indexable terms
	tokenize func([]byte) [][]byte

	// segmentSize specifies a maximum area of a file to be indexed as a single unit
	segmentSize uint64
	// concurrency sets how many go-routines are allowed for ingesting
	concurrency int
	// ctx allows to gracefully stop ingesting upon server shut-down
	ctx context.Context
}

type ScannedTokenizedMessage struct {
	scanner.MessageLayout
	DateTime time.Time
	Terms    [][]byte
	Err      error
}

func NewIngest(
	ctx context.Context,
	scan func(file string, locations []common.Location) ([]scanner.MessageLayout, error),
	parseTime func([]byte) (time.Time, error),
	tokenize func([]byte) [][]byte,
	db *db.DbContainer,
	ii *inverted_index_2.InvertedIndex,
	segmentSize uint64,
	concurrency int,
) *Ingest {
	ing := Ingest{
		sdb: db.SegmentsDb,
		fdb: db.FilesDb,
		mdb: db.MessagesDb,
		ii:  ii,

		findMessages: scan,
		parseTime:    parseTime,
		tokenize:     tokenize,

		segmentSize: segmentSize,
		concurrency: concurrency,
		ctx:         ctx,
	}
	return &ing
}

// Index concurrently indexes new areas in the given files
func (ing *Ingest) Index(files []string) error {

	// for cold startup when many files are to be indexed,
	// indexing happens concurrently to speed up warming the index.

	wg := errgroup.Group{}
	wg.SetLimit(ing.concurrency)

	// Each file goes in a separate go-routine, so if it fails (a file has been deleted or other reasons),
	// the rest of indexing is unaffected.
	for _, filePath := range files {
		wg.Go(func() (err error) {

			fileId, err := ing.fdb.GetFileId(filePath)
			if err != nil {
				return fmt.Errorf("index file: %w", err)
			}
			file := db.File{Path: filePath, Id: fileId}

			// Indexing flow:
			// 1. Extract unindexed messages
			locations, err := ing.selectLocationsForIndexing(file)
			if err != nil {
				return fmt.Errorf("index file: %w", err)
			}

			messagesCh := make(chan *ScannedTokenizedMessage, 1000)
			go func() {
				defer close(messagesCh)
				msgIterator, err := ing.extractMessages(filePath, locations)
				if err != nil {
					messagesCh <- &ScannedTokenizedMessage{Err: fmt.Errorf("index file: %w", err)}
					return
				}

				// allow concurrent reads and saves
				for s := range msgIterator {
					messagesCh <- s
				}
			}()

			// 2. Save to the storage
			_, err = ing.saveStream(file, messagesCh)
			if err != nil {
				return fmt.Errorf("index file %s: %w", file.Path, err)
			}
			return
		})
	}
	return wg.Wait()
}

// indexFile indexes the given areas in the file
func (ing *Ingest) extractMessages(filePath string, locations []common.Location) (iter.Seq[*ScannedTokenizedMessage], error) {

	if len(locations) == 0 {
		return common.NopSeq[*ScannedTokenizedMessage](), nil // nothing to index
	}

	// file indexing goes over segments one-by-one
	reader, err := os.Open(filePath)
	if err != nil {
		return common.NopSeq[*ScannedTokenizedMessage](), fmt.Errorf("extract messages: %w", err)
	}

	// it is quicker to read all message locations at once that call it for every location
	allMessageLayouts, err := ing.findMessages(filePath, locations)
	if errors.Is(err, scanner.NoMessageStartFound) || len(allMessageLayouts) == 0 {
		// no new messages found in the file
		return common.NopSeq[*ScannedTokenizedMessage](), nil
	} else if err != nil {
		return common.NopSeq[*ScannedTokenizedMessage](), fmt.Errorf("extract messages: %w", err)
	}

	iterator := func(yield func(message *ScannedTokenizedMessage) bool) {
		defer func() { _ = reader.Close() }()

		for _, location := range locations {
			locMessageLayouts := selectLocationLayouts(location, allMessageLayouts)
			if len(locMessageLayouts) == 0 {
				continue
			}

			for s := range ing.readMessagesInStream(filePath, reader, locMessageLayouts) {
				if !yield(s) {
					return
				}
			}
		}
	}

	return iterator, nil
}

// saveStream analyzes incoming (ordered by file position) messages, batches them into segments,
// saves the indexed values to the DB (as well as in II).
func (ing *Ingest) saveStream(file db.File, messages chan *ScannedTokenizedMessage) (lastSegmentLoc common.Location, err error) {

	// As we iterate through incoming messages we pick a segment they should be appended to:
	// - it could be an existing half-full segment
	// - or a new segment if others are full.
	// Each message will extend the segment's boundary (min/max time, start/end pos),
	// This calculation is done at the end of one segment's ingestion.

	segBuf := NewSegmentBuffer(file, ing.segmentSize, ing.sdb, ing.ii)
	defer func() {
		err2 := segBuf.flush()
		if err == nil {
			err = err2
		}
	}()

	var isNew bool
	for message := range messages {
		if message.Err != nil {
			err = fmt.Errorf("message scan failed: %w", message.Err)
			return
		}

		err, isNew = segBuf.Accept(message)
		if err != nil {
			err = fmt.Errorf("segment selection failed: %w", err)
			return
		}
		if isNew {
			ing.mdb.Flush()
		}

		relDateFrom := uint8(message.DateFrom - message.From)
		dateLen := uint8(message.DateTo - message.DateFrom)

		checkInErr := ing.mdb.CheckinMessage(segBuf.s.Id, message.From, relDateFrom, dateLen)
		if checkInErr != nil {
			err = fmt.Errorf("checking messages failed: %w", checkInErr)
			return
		}
	}

	lastSegmentLoc = segBuf.s.Loc
	return
}

// selectLocationsForIndexing returns contiguous file runs that were never indexed.
// Excludes segments that were previously checked in.
func (ing *Ingest) selectLocationsForIndexing(file db.File) ([]common.Location, error) {

	fileSize, err := common.FileSize(file.Path)
	if err != nil {
		return nil, fmt.Errorf("filesize failed for %s: %w", file.Path, err)
	}

	indexedLocations, err := ing.sdb.ReadIndexedLocations(file.Id)
	if err != nil {
		return nil, err
	}

	unindexedLocations := common.ExcludeLocations(common.Location{From: 0, To: fileSize}, indexedLocations...)
	return unindexedLocations, nil
}

// readMessagesInStream reads messages from the file stream at the given positions (see layouts),
// each message is tokenized.
func (ing *Ingest) readMessagesInStream(name string, stream io.ReaderAt, messageLayouts []scanner.MessageLayout) iter.Seq[*ScannedTokenizedMessage] {

	// Big messages lead to an exponential memory consumption spike,
	// so to be predictable in terms of memory, we limit the size of indexable area.
	maxIndexableSize := uint64(10_000_000) // bytes

	iterator := func(yield func(message *ScannedTokenizedMessage) bool) {
		buf := make([]byte, 0)
		var err error
	loop:
		for _, layout := range messageLayouts {

			select {
			case <-ing.ctx.Done():
				break loop // stop
			default:
			}

			messageLen := layout.To - layout.From
			if messageLen > maxIndexableSize {
				common.Out("big message %dMiB at %s:%d", messageLen/1024/1024, name, layout.From)

				// Index only the beginning and the end of the big message.
				halfSize := maxIndexableSize / 2
				if cap(buf) < int(maxIndexableSize) {
					buf = make([]byte, maxIndexableSize)
				}

				// Read the beginning of the message
				_, err := stream.ReadAt(buf[:halfSize], int64(layout.From))
				if err != nil {
					err = fmt.Errorf("file read: %w", err)
					if !yield(&ScannedTokenizedMessage{Err: err}) {
						break
					}
				}

				// Read the end of the message
				_, err = stream.ReadAt(buf[halfSize:], int64(layout.To-halfSize))
				if err != nil {
					err = fmt.Errorf("file read: %w", err)
					if !yield(&ScannedTokenizedMessage{Err: err}) {
						break
					}
				}

			} else {
				// Messages under the limit are indexed entirely.
				if cap(buf) < int(messageLen) {
					buf = make([]byte, messageLen)
				} else {
					buf = buf[:messageLen]
				}

				_, err := stream.ReadAt(buf, int64(layout.From))
				if err != nil {
					err = fmt.Errorf("file read: %w", err)
					if !yield(&ScannedTokenizedMessage{Err: err}) {
						break
					}
				}
			}

			dateFrom, dateTo := layout.DateFrom-layout.From, layout.DateTo-layout.From
			terms := ing.tokenize(buf[:dateFrom]) // slow
			terms = append(terms, ing.tokenize(buf[dateTo:])...)

			tm := &ScannedTokenizedMessage{
				MessageLayout: layout,
				Terms:         terms,
			}

			tm.DateTime, err = ing.parseTime(buf[dateFrom:dateTo])
			if err != nil {
				err = fmt.Errorf("date parse: %w", err)
				if !yield(&ScannedTokenizedMessage{Err: err}) {
					break
				}
			}

			if !yield(tm) {
				break
			}
		}
	}

	return iterator
}

// From the list of all layouts select suitable for the given location.
func selectLocationLayouts(loc common.Location, layouts []scanner.MessageLayout) []scanner.MessageLayout {
	leftLayout := scanner.MessageLayout{From: loc.From}
	rightLayout := scanner.MessageLayout{From: loc.To}

	lpos, _ := slices.BinarySearchFunc(layouts, leftLayout, func(a, b scanner.MessageLayout) int {
		return int(a.From - b.From)
	})
	if lpos >= len(layouts) {
		return layouts[len(layouts):]
	}

	rpos, _ := slices.BinarySearchFunc(layouts, rightLayout, func(a, b scanner.MessageLayout) int {
		return int(a.From - b.From)
	})

	return layouts[lpos:rpos]
}
