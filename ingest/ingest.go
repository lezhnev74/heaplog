package ingest

import (
	"database/sql"
	"errors"
	"github.com/lezhnev74/inverted_index_2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/scanner"
	"io"
	"log"
	"os"
	"slices"
	"time"
	"unsafe"
)

var EmptySegment = errors.New("no messages begins in the segment")

type Ingest struct {
	// findMessages extracts message layouts (boundaries) from the file
	findMessages func(file string) ([]scanner.MessageLayout, error)
	parseTime    func([]byte) (time.Time, error)
	tokenize     func([]byte) [][]byte
	db           *db.DbContainer
	ii           *inverted_index_2.InvertedIndex
	segmentSize  uint64
	concurrency  int // the level of concurrency in ingestion
}

type ScannedTokenizedMessage struct {
	scanner.MessageLayout
	DateTime time.Time
	Terms    [][]byte
	Err      error
}

func NewIngest(
	scan func(file string) ([]scanner.MessageLayout, error),
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

	allMessageLayouts, err := ing.findMessages(file)

	// pickNextLocation returns the next contiguous run (that is at most segmentSize long)
	pickNextLocation := func(minPos uint64) (nextLoc common.Location) {
		for _, l := range locations {
			if l.Contains(minPos) {
				nextLoc = common.Location{minPos, min(minPos+ing.segmentSize, l.To)}
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

	for {
		loc := pickNextLocation(lastSegmentLoc.To)
		if loc.Len() == 0 {
			break
		}

		locMessageLayouts := selectLayouts(loc, allMessageLayouts)
		if len(locMessageLayouts) == 0 {
			// here is the workaround for files where messages begin not from the beginning
			// in which case a location may have no messages at all.
			lastSegmentLoc.To++
			continue
		}

		tokenizedMessages := ing.readMessagesInStream(reader, locMessageLayouts)
		lastSegmentLoc, err = ing.saveBatch(file, tokenizedMessages)
		if err != nil {
			return xerrors.Errorf("save segment: %w", err)
		}
	}

	ing.db.MessagesDb.Flush()

	return nil
}

// saveBatch analyzes incoming messages and saves them as a segment to the DB (as well as in II).
// If indexing happens more often then messages appear in the file, then segments can be very small (and pollute II).
// To solve that it must stream found messages to the existing segment (that adjoins this message)
// until it is full, in which case it should allocate a new one.
func (ing *Ingest) saveBatch(file string, messages <-chan *ScannedTokenizedMessage) (segmentLoc common.Location, err error) {
	t0 := time.Now()

	fileId, err := ing.db.GetFileId(file)
	if err != nil {
		err = xerrors.Errorf("index file: %w", err)
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
		// update segment boundaries
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

		curSegmentTermsMap = make(map[string]struct{}) // reset for the next segment

		// Report
		if curSegmentMessages > 0 {
			log.Printf("indexed %s[%d:%d]: %d messages, %d terms in %s", file, curSegment.Loc.From, curSegment.Loc.To, curSegmentMessages, len(segmentTerms), time.Now().Sub(t0).String())
		}

		t0 = time.Now()
		return nil
	}

	// selectSegment picks a segment which is half-full and adjoins this message,
	// otherwise it starts a new segment
	selectSegment := func(m *ScannedTokenizedMessage) (segmentId uint32, err error) {
		if curSegment.Id == 0 {
			// this is the time the selection is invoked, so we try to use an existing segment
			// that adjoins this message. It can return 0 if none found, though.
			curSegment, err = ing.db.SelectSegmentThatAdjoins(fileId, m.From)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err // no suitable segments is an expected case
				}
			}
		}

		if curSegment.Id == 0 || uint64(curSegment.Loc.Len()) > ing.segmentSize {
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
			curSegment.Loc = common.Location{m.From, m.To}
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
			if errors.Is(message.Err, scanner.NoMessageStartFound) {
				err = EmptySegment
			} else {
				err = xerrors.Errorf("save segment: %w", message.Err)
			}
			return // no more messages
		}

		segmentId, serr := selectSegment(message)
		if serr != nil {
			err = xerrors.Errorf("segment selection: %w", serr)
			return
		}

		relDateFrom := uint8(message.DateFrom - message.From)
		dateLen := uint8(message.DateTo - message.DateFrom)

		checkInErr := ing.db.CheckinMessage(segmentId, message.From, relDateFrom, dateLen)
		if checkInErr != nil {
			err = xerrors.Errorf("checking messages: %w", checkInErr)
			return
		}

		for i := range message.Terms {
			t := unsafe.String(unsafe.SliceData(message.Terms[i]), len(message.Terms[i]))
			curSegmentTermsMap[t] = struct{}{}
		}
	}

	err = flush()
	segmentLoc = curSegment.Loc
	return
}

func (ing *Ingest) readMessagesInStream(file io.ReaderAt, messageLayouts []scanner.MessageLayout) <-chan *ScannedTokenizedMessage {
	r := make(chan *ScannedTokenizedMessage)

	go func() {
		defer close(r)

		buf := make([]byte, 1000)
		for _, layout := range messageLayouts {
			messageLen := layout.To - layout.From
			if cap(buf) < int(messageLen) {
				buf = make([]byte, messageLen)
			} else {
				buf = buf[:messageLen]
			}

			_, err := file.ReadAt(buf, int64(layout.From))
			if err != nil {
				err = xerrors.Errorf("file read: %w", err)
				r <- &ScannedTokenizedMessage{Err: err}
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
func selectLayouts(loc common.Location, layouts []scanner.MessageLayout) []scanner.MessageLayout {
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
