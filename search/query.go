package search

import (
	"errors"
	"fmt"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
	"heaplog/common"
	"heaplog/scanner"
	"heaplog/storage"
	"io"
	"log"
	"sync"
	"time"
	"unsafe"
)

var (
	ErrNoQuery = errors.New("Query not found")
)

type QueryBuildingResult struct {
	Id             string
	QueryComplete  bool
	FirstPageReady bool
	Err            error
}

// QuerySearch is a top-level service that searches indexed files.
// It tries to use the index to limit amount of reads(+scans).
type QuerySearch struct {
	selector *SegmentsSelector
	storage  *storage.Storage
	scanner  *scanner.Scanner
}

func NewQuerySearch(
	selector *SegmentsSelector,
	storage *storage.Storage,
	_scanner *scanner.Scanner,
) *QuerySearch {
	return &QuerySearch{
		selector: selector,
		storage:  storage,
		scanner:  _scanner,
	}
}

// NewQuery will do a new search and save results to the duckdb (heavy).
// The searching is a background process that communicates signals:
// - if the error happened (the query is discarded and search stops)
// - when the first page is ready (that is to help send results to the user early)
// - when search is done and all results are available for reading.
func (s *QuerySearch) NewQuery(query string, minDate, maxDate *time.Time, pageSize int) (
	string,
	<-chan QueryBuildingResult,
	error,
) {
	// A scheduler reads from the selection tree:
	//  - it uses a pool of file descriptors and waits for a slot to be available (throttles open descriptors)
	//  - once acquired a slot it reuses or creates a new file descriptor for the segment and starts a goroutine.
	//  - after each segment scan complete it checks if a full page of results is ready and reports that.
	//  - it maintains a cache of file descriptors, so it will reuse those or evict(close) one and open a new one.

	// the channel should have a buffer so the client won't block this
	// 3 possible messages: page ready + complete, or an error
	results := make(chan QueryBuildingResult, 3)

	expr, err := ParseUserQuery(query)
	if err != nil {
		return "", nil, err
	}

	minDateMicro, maxDateMicro := int64(0), int64(0)
	if minDate != nil {
		minDateMicro = minDate.UnixMicro()
	}
	if maxDate != nil {
		maxDateMicro = maxDate.UnixMicro()
	}
	queryHash := common.HashString(fmt.Sprintf("%s%d%d", expr.Hash(), minDateMicro, maxDateMicro))

	// check if the query exists already
	_, err = s.storage.GetQuerySummary(queryHash, nil, nil)
	if err == nil {
		// return early
		go func() {
			defer close(results)
			results <- QueryBuildingResult{queryHash, false, true, nil}
			results <- QueryBuildingResult{queryHash, true, false, nil}
		}()
		log.Printf("query %s: already exists", queryHash)
		return queryHash, results, nil
	}

	bodyMatcher := expr.getMatcher()
	matcher := func(sm *scanner.ScannedMessage) bool {
		if minDate != nil && sm.DateTime.Before(*minDate) {
			return false
		}
		if maxDate != nil && sm.DateTime.After(*maxDate) {
			return false
		}

		bodyString := unsafe.String(unsafe.SliceData(sm.Body), len(sm.Body))
		return bodyMatcher(bodyString)
	}

	err = s.storage.CheckInQuery(queryHash, query, minDate, maxDate)
	if err != nil {
		return "", nil, err
	}

	segmentIds, err := s.selector.SelectSegments(expr, minDate, maxDate)
	if err != nil {
		return "", nil, err
	}

	log.Printf("query %s: selected %d segments", queryHash, len(segmentIds))

	go func() {
		defer close(results)

		// Algorithm to check if the first page is available:
		// 1. Read out all segments (from,to,matchedCount) into a slice (sorted by minDate).
		// 	  set matchedCount to "-1" for unprocessed segments.
		// 2. Process each segment concurrently.
		// 3. After each segment is processes update its matchedCount to >-1 in the slice (means it was processed).
		// 4. Count all matchedCount of segments until the first unprocessed (the one with matchedCount==-1).
		// 5. If the count is >= pageSize -> return the first page result.

		segmentInfos, err := s.storage.ReadSegmentsInfo(segmentIds)
		if err != nil {
			results <- QueryBuildingResult{Err: err}
			return
		}
		for i := range segmentInfos {
			segmentInfos[i].Messages = -1 // reset all segments to indicate "unindexed"
		}
		segmentInfosLock := sync.Mutex{}
		firstPageReady := false
		segmentDone := func(s common.IndexedSegmentInfo, matchedMessages int) {
			// here we confirm each segment and check for the first page

			segmentInfosLock.Lock()
			defer segmentInfosLock.Unlock()

			for i, ss := range segmentInfos {
				if s.Id != ss.Id {
					continue
				}

				segmentInfos[i].Messages = int64(matchedMessages)
				break
			}

			// now check if the page is ready
			if firstPageReady {
				return
			}

			totalMatchedMessages := int64(0)
			for _, s := range segmentInfos {
				if s.Messages < 0 { // stop at the first unindexed segment
					break
				}
				totalMatchedMessages += s.Messages
			}

			if totalMatchedMessages >= int64(pageSize) {
				firstPageReady = true

				log.Printf("query %s: first page ready", queryHash)
				results <- QueryBuildingResult{FirstPageReady: true}
			}
		}

		segmentInfosCh := make(chan common.IndexedSegmentInfo)

		matchSegmentsMessages := func(si common.IndexedSegmentInfo, segmentBuf []byte) error {
			filePath, err := s.storage.ReadFileByHash(si.DataSource)
			if err != nil {
				return err
			}

			mr, err := mmap.Open(filePath)
			if err != nil {
				return err
			}
			defer mr.Close()

			_, err = mr.ReadAt(segmentBuf, si.From)
			if err != nil {
				return err
			}

			segments, err := s.storage.GetSegments([]int{int(si.Id)})
			if err != nil {
				return err
			}
			segment := segments[0]

			// match messages in the segment
			matchedMessages := 0
			for _, m := range segment.Messages {
				sm := scanner.ScannedMessage{
					Body:     segmentBuf[m.Loc.Min-si.From : m.Loc.Max-si.From], // position within the segment
					Date:     nil,
					Pos:      int(m.Loc.Min),
					DateTime: m.Date,
					Err:      nil,
				}

				if !matcher(&sm) {
					continue
				}

				matchedMessages++
				message := common.MatchedMessage{
					Id:        m.Id,
					QueryHash: queryHash,
				}

				s.storage.CheckInQueryMessage(message)
			}

			segmentDone(si, matchedMessages)

			return nil
		}

		workers := errgroup.Group{}
		for i := 0; i < 10; i++ { // each segment is loaded into memory entirely

			// now spawn a worker to scan a segment in a stream (descriptor)
			workers.Go(func() (err error) {
				segmentBuf := make([]byte, 0) // re-use buffer for the segment
				for segmentInfo := range segmentInfosCh {

					segmentSize := segmentInfo.To - segmentInfo.From
					if len(segmentBuf) < int(segmentSize) {
						segmentBuf = make([]byte, segmentSize)
					}

					err = matchSegmentsMessages(segmentInfo, segmentBuf[:segmentSize])
					if err != nil {
						return err
					}
				}

				return nil
			})
		}

		for _, segmentInfo := range segmentInfos {
			segmentInfosCh <- segmentInfo
		}
		close(segmentInfosCh)

		err = workers.Wait()
		if err != nil {
			results <- QueryBuildingResult{Err: err}
			return
		}

		err = s.storage.CheckInFinishedQuery(queryHash)
		if err != nil {
			results <- QueryBuildingResult{Err: err}
			return
		}

		log.Printf("query %s: complete", queryHash)
		results <- QueryBuildingResult{QueryComplete: true}
	}()

	return queryHash, results, nil
}

// ReadMessages reads actual messages from the heap-files from provided locations.
func (s *QuerySearch) ReadMessages(messages []common.MatchedMessage) ([][]byte, error) {
	concurrentReads := 10
	pool := common.NewStreamsPool(concurrentReads)
	freeSlots := make(chan bool, concurrentReads)
	ret := make([][]byte, len(messages))

	wg := sync.WaitGroup{}
	wg.Add(len(messages))
	for i, m := range messages {
		i, m := i, m

		// allocate memory for the message
		ret[i] = make([]byte, m.Loc.Max-m.Loc.Min)

		freeSlots <- true
		go func() {
			defer func() { wg.Done() }()
			defer func() { <-freeSlots }()

			filePath, err := s.storage.ReadFileByHash(m.DataSource)
			if err != nil {
				return
			}

			stream, err := pool.Get(filePath)
			if err != nil {
				return
			}

			_, err = stream.Seek(m.Loc.Min, io.SeekStart)
			if err != nil {
				return
			}

			_, err = stream.Read(ret[i])
			if err != nil {
				return
			}
		}()
	}
	wg.Wait()

	return ret, nil
}
