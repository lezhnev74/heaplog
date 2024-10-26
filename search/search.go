package search

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2"
	"golang.org/x/xerrors"
	"heaplog_2024/db"
	"heaplog_2024/query_language"
	"log"
	"slices"
	"sync"
	"time"
)

type Search struct {
	db *db.DbContainer
	ii *inverted_index_2.InvertedIndex
	// dateFormat is GO time format for message's dates,
	// extract message date upon scanning heap files
	dateFormat string
	ctx        context.Context
}

type SearchMatcher func(m db.Message, body []byte) bool

func NewSearch(ctx context.Context, db *db.DbContainer, ii *inverted_index_2.InvertedIndex, dateFormat string) *Search {
	return &Search{
		db:         db,
		ii:         ii,
		dateFormat: dateFormat,
		ctx:        ctx,
	}
}

// Search is the main gateway to the message-matching functionality.
// Given the user query expression, it decides if the inverted index can be used
// to reduce the amount of messages to test.
// It streams out matched messages.
func (s *Search) Search(
	expr *query_language.Expression,
	min, max *time.Time,
	dateFormat string,
	tokenize func([]byte) [][]byte,
	concurrency int,
) (matchedIt go_iterators.Iterator[db.Message], isFullScan bool, err error) {
	segments, err := s.db.AllSegmentIds(min, max)
	if err != nil {
		err = xerrors.Errorf("all segments query: %w", err)
		return
	}

	isFullScan = ShouldFullScan(expr, tokenize)
	if !isFullScan {
		// Use Inverted Index to reduce potential segments to scan.
		// Segments are coming from the II sorted, which is OK as
		// ingestion assigns segment ids (within the same file) in the same order.
		segments, err = s.filterSegmentsWithInvertedIndex(expr, segments, tokenize)
		if err != nil {
			err = xerrors.Errorf("inverted index failure: %w", err)
			return
		}
		fmt.Printf("Selected segments: %d\n", len(segments))
	}

	exprMatcher := expr.GetMatcher()
	matcher := func(m db.Message, body []byte) bool {

		// put the date to the message (saving search results needs it)
		t, err := time.Parse(dateFormat, string(body[m.RelDateLoc.From:m.RelDateLoc.From+m.RelDateLoc.To]))
		if err != nil {
			return false
		}
		m.Date = &t

		if (min != nil && m.Date.Before(*min)) || (max != nil && m.Date.After(*max)) {
			return false
		}

		// exclude date from matching
		body = append(body[:m.RelDateLoc.From], body[m.RelDateLoc.From+m.RelDateLoc.To:]...)

		result := exprMatcher(string(body))

		return result
	}

	// Sequential read of segments is not efficient, we can benefit from concurrency here.
	// However, the order of messages is important, so we need to emit matched messages in the same order as given segments (ordered by date).
	// We can create a buffer where for each segment there is an array of matched messages, thus each segment can be
	// matched concurrently, while the emitting logic will only stream out events in the order of segments.

	// Here we keep all messages for all segments (we can release slices once they were streamed out)
	segmentsResults := make([][]db.Message, len(segments))
	segmentsResultsLock := sync.Mutex{}
	// wakes up when a new segment is ready with matched messages
	segmentsResultsCondvar := sync.NewCond(&segmentsResultsLock)
	freeList := make(chan bool, concurrency)

	// This function starts in the background and reads out all message rows from the db
	// It does so concurrently, so the matcher does not wait for the data to read-and-match
	go func() {

		messagesSelect := `
		SELECT m.*, s.posTo as lastMessageTo, s.fileId   
		FROM file_segments_messages m
		JOIN file_segments s ON m.segmentId=s.id
		WHERE m.segmentId=?
		ORDER BY m.posFrom ASC 
		`
		stmt, err := s.db.Prepare(messagesSelect)
		if err != nil {
			panic(err)
		}

	rangeSegments:
		for i, segment := range segments {

			select {
			case <-s.ctx.Done():
				// Cancellation test: after another segment is checked we test the context,
				// if cancelled, stop processing.
				break rangeSegments
			case freeList <- true:
				// get the slot and process the segment
			}

			go func() {
				defer func() {
					<-freeList // release the slot
				}()
				segmentMessagesIt, err := s.db.IterateRowsFromStatement(stmt, []any{segment})
				if err != nil {
					log.Fatalf("search segment: %s", err)
					return
				}
				defer segmentMessagesIt.Close()

				// Instead of just keeping potential messages here, we can run filtering,
				// so results contains only matched messages.
				matchedMessages := []db.Message{}
				matched, err := s.FilterMessagesStream(segmentMessagesIt, matcher)
				defer matched.Close()
				for {
					m, err := matched.Next()
					if errors.Is(err, go_iterators.EmptyIterator) {
						break
					} else if err != nil {
						log.Printf("unable to match message: %s", err)
						break
					}
					matchedMessages = append(matchedMessages, m)
				}

				segmentsResultsLock.Lock()
				segmentsResults[i] = matchedMessages
				segmentsResultsCondvar.Signal()
				segmentsResultsLock.Unlock()

				return
			}()
		}
	}()

	// This iterator returns messages from each segment sequentially (sorted by segment min date)
	// so the output stream contains messages sorted.
	curSegmentIndex := 0
	matchedIt = go_iterators.NewSequentialDynamicIterator(func() (go_iterators.Iterator[db.Message], error) {
		if curSegmentIndex == len(segmentsResults) {
			return nil, go_iterators.EmptyIterator
		}
		// we must wait until the next segment is processed, so we maintain the order of messages
		var curSegmentResults []db.Message
		segmentsResultsLock.Lock()

		// check if it available already before going to sleep-wait
		curSegmentResults = segmentsResults[curSegmentIndex]

		for curSegmentResults == nil {
			segmentsResultsCondvar.Wait()
			curSegmentResults = segmentsResults[curSegmentIndex]
			if curSegmentResults != nil {
				break
			}
		}

		segmentsResults[curSegmentIndex] = nil //gc
		curSegmentIndex++
		segmentsResultsLock.Unlock()

		return go_iterators.NewSliceIterator(curSegmentResults), nil
	})

	return
}

// FullScan will read all messages registered in the system (indexed) and apply match func to each
func (s *Search) FullScan(matchFunc SearchMatcher) (go_iterators.Iterator[db.Message], error) {
	messages, err := s.db.AllMessagesIt()
	if err != nil {
		return nil, err
	}
	return s.FilterMessagesStream(messages, matchFunc)
}

func (s *Search) FilterFile(file string, messages []db.Message, matchFunc SearchMatcher) (matched []db.Message, err error) {

	fileIterator, err := StreamFileMatch(file, messages, matchFunc, s.dateFormat)
	if err != nil {
		return nil, xerrors.Errorf("scan: %w", err)
	}
	defer fileIterator.Close()

	for {
		message, err := fileIterator.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		matched = append(matched, message)
	}

	return
}

// FilterMessagesStream accepts messages to match, groups it by file and scans for their bytes from the heapfiles.
// Matched messages are streamed out.
func (s *Search) FilterMessagesStream(messages go_iterators.Iterator[db.Message], matchFunc SearchMatcher) (go_iterators.Iterator[db.Message], error) {

	messagesBySegment := go_iterators.NewGroupingIterator(
		messages,
		func(m db.Message) any { return m.SegmentId },
	)

	var (
		fileIterator go_iterators.Iterator[db.Message]
		batch        []db.Message
	)

	it := go_iterators.NewCallbackIterator(
		func() (matched db.Message, err error) {

			for {
				if fileIterator == nil {
					// All incoming messages must be grouped by the segment,
					// so we can read them efficiently. If there is no file iterator,
					// we accumulate messages (batch) for the same file until another message is found (or eof).
					batch, err = messagesBySegment.Next()
					if err != nil {
						break
					}

					file, err2 := s.db.GetFile(batch[0].FileId)
					if err2 != nil {
						err = xerrors.Errorf("scan: %w", err2)
						return
					}

					fileIterator, err = StreamFileMatch(file, batch, matchFunc, s.dateFormat)
					if err != nil {
						err = xerrors.Errorf("scan: %w", err)
						return
					}
				}

				matched, err = fileIterator.Next()
				if errors.Is(err, go_iterators.EmptyIterator) {
					fileIterator.Close()
					fileIterator = nil
					continue
				}

				return
			}

			err = go_iterators.EmptyIterator
			return
		},
		func() error {
			if fileIterator != nil {
				fileIterator.Close()
			}
			return messages.Close()
		},
	)
	return it, nil
}

func (s *Search) filterSegmentsWithInvertedIndex(expr *query_language.Expression, allSegments []uint32, tokenize func([]byte) [][]byte) ([]uint32, error) {
	expr = expr.Clone()
	// Literals are a collection of normal prefix-terms in the expression "err failure" => [err, failure]
	// short terms can't be used in the II lookup, and must be treated as Full-Scan, as well as RE literals.
	literals := expr.FindKeywords()
	terms := make([][]byte, 0, len(literals))
	for _, lit := range literals {
		for _, t := range tokenize([]byte(lit)) {
			terms = append(terms, t)
		}
	}
	slices.SortFunc(terms, bytes.Compare)
	terms = slices.CompactFunc(terms, bytes.Equal)

	// iiTermValues contain allSegments for long term prefixes found in the expression
	iiTermValues, err := s.ii.PrefixSearch(terms)
	if err != nil {
		return nil, xerrors.Errorf("ii prefix: %w", err)
	}

	// Now we can map the whole expression to sets of segment (prepare for evaluation)
	// RE literals and short terms are Full-Scan (all allSegments set), others map to II lookup results.
	ExprMapLiteralsToSets(expr, tokenize, iiTermValues, allSegments)

	matchedSegments := ExprEval(expr, allSegments)
	return matchedSegments, nil
}
