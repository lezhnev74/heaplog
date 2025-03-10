package ingest

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"slices"
	"time"
	"unsafe"

	"github.com/lezhnev74/inverted_index_2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/scanner"
)

type Ingest struct {
	// findMessages extracts message layouts (boundaries) from the file
	findMessages func(file string, locations []common.Location) ([]scanner.MessageLayout, error)
	parseTime    func([]byte) (time.Time, error)
	tokenize     func([]byte) [][]byte
	db           *db.DbContainer
	ii           *inverted_index_2.InvertedIndex
	segmentSize  uint64
	concurrency  int // the level of concurrency in ingestion
	ctx          context.Context
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
		findMessages: scan,
		parseTime:    parseTime,
		tokenize:     tokenize,
		db:           db,
		ii:           ii,
		segmentSize:  segmentSize,
		concurrency:  concurrency,
		ctx:          ctx,
	}
	return &ing
}

func (ing *Ingest) IndexConcurrent(files []string, concurrency int) error {
	ing.concurrency = concurrency
	return ing.Index(files)
}

func (ing *Ingest) Index(files []string) error {

	// for cold startup when many files are to be indexed, we can index each file concurrently.
	// otherwise under normal load, indexing a file can be done in one thread with no problems.

	wg := errgroup.Group{}
	wg.SetLimit(ing.concurrency)

	// Each file goes in a separate go-routine, so if it fails (a file has been deleted or other reasons),
	// the rest of indexing is unaffected.
	for _, file := range files {
		wg.Go(func() (err error) {
			locations, err := SelectLocationsForIndexing(ing.db, file)
			if err != nil {
				return xerrors.Errorf("index: %w", err)
			}
			err = ing.indexFile(file, locations)
			if err != nil {
				return xerrors.Errorf("index file: %w", err)
			}
			return
		})
	}
	return wg.Wait()
}

func (ing *Ingest) indexFile(file string, locations []common.Location) error {

	if len(locations) == 0 {
		return nil // nothing to index
	}

	fileId, err := ing.db.GetFileId(file)
	if err != nil {
		return xerrors.Errorf("index file: %w", err)
	}

	// file indexing goes over segments one-by-one
	reader, err := os.Open(file)
	if err != nil {
		return xerrors.Errorf("index file: %w", err)
	}
	defer reader.Close()

	allMessageLayouts, err := ing.findMessages(file, locations)
	if errors.Is(err, scanner.NoMessageStartFound) || len(allMessageLayouts) == 0 {
		return xerrors.Errorf("no messages found")
	} else if err != nil {
		return xerrors.Errorf("message scan failed: %w", err)
	}

	// pickNextLocation returns the next contiguous run (that is at most segmentSize long)
	pickNextLocation := func(minPos uint64) (nextLoc common.Location) {
		for _, l := range locations {
			if l.Contains(minPos) {
				nextLoc = common.Location{From: minPos, To: min(minPos+ing.segmentSize, l.To)}
				break
			}
		}
		return
	}

	lastSegmentLoc, err := ing.db.LastSegmentLocation(fileId)
	if err != nil {
		err = xerrors.Errorf("index file: %w", err)
		return err
	}

	loops := 0
	for {
		loc := pickNextLocation(lastSegmentLoc.To)
		if loc.Len() == 0 {
			break
		}

		locMessageLayouts := selectLocationLayouts(loc, allMessageLayouts)
		if len(locMessageLayouts) == 0 {
			// here is the workaround for files where messages begin not from the beginning
			// in which case a location may have no messages at all.
			lastSegmentLoc.To++
			continue
		}

		tokenizedMessages := ing.readMessagesInStream(file, reader, locMessageLayouts)
		lastSegmentLoc, err = ing.saveBatch(file, tokenizedMessages)
		if err != nil {
			return xerrors.Errorf("save segment failed: %w", err)
		}

		loops++
	}

	ing.db.MessagesDb.Flush()

	return nil
}

// saveBatch analyzes incoming messages and saves them as a segment to the DB (as well as in II).
// If indexing happens more often then messages appear in the file, then segments can be very small (and pollute II).
// To solve that it must stream found messages to the existing segment (that adjoins this message)
// until it is full, in which case it should allocate a new one.
func (ing *Ingest) saveBatch(file string, messages <-chan *ScannedTokenizedMessage) (lastSegmentLoc common.Location, err error) {
	t0 := time.Now()

	fileId, err := ing.db.GetFileId(file)
	if err != nil {
		err = xerrors.Errorf("file is missing: %w", err)
		return
	}

	// As we iterate through incoming messages we pick a segment they should be appended to:
	// - it could be an existing half-full segment
	// - or a new segment if others are full.
	// Each message will extend the segment's boundary (min/max time, start/end pos),
	// This calculation is done at the end of one segment's ingestion.

	curSegment := db.Segment{}
	curSegmentTermsMap := map[string]struct{}{} // for deduplication
	curSegmentMessages := 0

	// As we iterate through messages, we accumulate segment terms.
	// This function flushes terms to II and updates cur segment's boundaries
	flush := func() error {

		// Here it has to save data with respect to accidental interruptions.
		// The reserved segment id is used in both messages and II. But in case the interruption happens
		// during the flush, those will point to a non-existing segment.
		// In some cases that can lead to disk space wasted.
		// Saving the segment as the last step gives more guarantees that no empty segments will appear.

		// Before proceeding, we need to make sure the messages are flushed (though this is a non-blocking operation)
		ing.db.MessagesDb.Flush()

		// update II
		segmentTerms := make([][]byte, 0, len(curSegmentTermsMap))
		for i := range curSegmentTermsMap {
			b := unsafe.Slice(unsafe.StringData(i), len(i))
			segmentTerms = append(segmentTerms, b)
		}
		iiErr := ing.ii.Put(segmentTerms, uint32(curSegment.Id))
		if iiErr != nil {
			return xerrors.Errorf("save segment: ii: %w", iiErr)
		}

		// as the last step, persist the segment
		syncErr := ing.db.CheckinSegmentWithId(
			uint32(curSegment.Id),
			fileId,
			curSegment.Loc,
			curSegment.DateMin,
			curSegment.DateMax,
		)
		if syncErr != nil {
			return xerrors.Errorf("sync segment: %w", syncErr)
		}

		// Report
		if curSegmentMessages > 0 {
			common.Out("indexed %s[%d:%d]: %d messages, %d terms in %s", file, curSegment.Loc.From, curSegment.Loc.To, curSegmentMessages, len(segmentTerms), time.Since(t0).String())
		}

		// Cleanup
		curSegmentTermsMap = make(map[string]struct{}) // reset for the next segment

		t0 = time.Now()
		return nil
	}

	// selectSegment picks a segment which is half-full and adjoins this message,
	// otherwise it starts a new segment.
	selectSegment := func(m *ScannedTokenizedMessage) (segmentId uint32, err error) {
		if curSegment.Id == 0 {
			// this is the first time the selection is invoked, so we try to use an existing segment
			// that adjoins this message. It can return 0 if none found, though.
			// "no suitable segment" is an expected case.
			curSegment, err = ing.db.SelectSegmentThatAdjoins(fileId, m.From)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
		}

		// make a new segment if the current is absent or too big already.
		if curSegment.Id == 0 || uint64(curSegment.Loc.Len()) > ing.segmentSize {
			// flush previous segment data
			if curSegment.Id != 0 {
				err = flush()
				if err != nil {
					return
				}
			}

			// start a new segment
			curSegment = db.Segment{}
			newId, _ := ing.db.ReserveSegmentId()
			curSegment.Id = int(newId)
			curSegment.FileId = fileId
			curSegment.Loc = common.Location{From: m.From, To: m.To}
			curSegment.DateMin = m.DateTime
			curSegment.DateMax = m.DateTime
			curSegmentMessages = 0
		}

		curSegment.Loc.To = m.To
		curSegment.DateMax = m.DateTime
		curSegmentMessages++

		return uint32(curSegment.Id), nil
	}

	for message := range messages {
		if message.Err != nil {
			err = xerrors.Errorf("message scan failed: %w", message.Err)
			return
		}

		segmentId, serr := selectSegment(message)
		if serr != nil {
			err = xerrors.Errorf("segment selection failed: %w", serr)
			return
		}

		relDateFrom := uint8(message.DateFrom - message.From)
		dateLen := uint8(message.DateTo - message.DateFrom)

		checkInErr := ing.db.CheckinMessage(segmentId, message.From, relDateFrom, dateLen)
		if checkInErr != nil {
			err = xerrors.Errorf("checking messages failed: %w", checkInErr)
			return
		}

		for i := range message.Terms {
			t := unsafe.String(unsafe.SliceData(message.Terms[i]), len(message.Terms[i]))
			curSegmentTermsMap[t] = struct{}{}
		}
	}

	err = flush()
	lastSegmentLoc = curSegment.Loc
	return
}

func (ing *Ingest) readMessagesInStream(name string, stream io.ReaderAt, messageLayouts []scanner.MessageLayout) <-chan *ScannedTokenizedMessage {
	r := make(chan *ScannedTokenizedMessage)

	// Big messages lead to an exponential memory consumption spike,
	// so to be predictable in terms of memory, we limit the size of indexable area.
	maxIndexableSize := uint64(10_000_000) // bytes

	go func() {
		defer close(r)

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
					err = xerrors.Errorf("file read: %w", err)
					r <- &ScannedTokenizedMessage{Err: err}
				}

				// Read the end of the message
				_, err = stream.ReadAt(buf[halfSize:], int64(layout.To-halfSize))
				if err != nil {
					err = xerrors.Errorf("file read: %w", err)
					r <- &ScannedTokenizedMessage{Err: err}
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
					err = xerrors.Errorf("file read: %w", err)
					r <- &ScannedTokenizedMessage{Err: err}
				}
			}

			dateFrom, dateTo := layout.DateFrom-layout.From, layout.DateTo-layout.From
			terms := ing.tokenize(buf[:dateFrom])
			terms = append(terms, ing.tokenize(buf[dateTo:])...)

			tm := &ScannedTokenizedMessage{
				MessageLayout: layout,
				Terms:         terms,
			}

			tm.DateTime, err = ing.parseTime(buf[dateFrom:dateTo])
			if err != nil {
				err = xerrors.Errorf("date parse: %w", err)
				r <- &ScannedTokenizedMessage{Err: err}
			}

			r <- tm
		}
	}()

	return r
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
