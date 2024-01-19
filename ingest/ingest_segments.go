package ingest

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"heaplog/common"
	"heaplog/indexer"
	"heaplog/scanner"
	"heaplog/storage"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

/*
The design of the ingestor is concurrent and consists of 3 pieces:
1. Segments producer (SP)
2. Worker Pool (WP)
3. Segment Worker (SW)

Segment Producer (SP).
SP uses our data storage to build a list of unindexed segments
based on the list of all files and previously indexed segments.
It pushes file segments to a channel consumed by WP.

Worker Pool (WP).
WP is a work controller that accepts tasks from SP and spreads them to SWs.
It waits for the result of each WP and handles failures.
We should anticipate that files can be removed at any point in time during this process.
It also maintains a pool of file descriptors to reuse (assuming incoming tasks are aligned to files).

Segment Worker (SW).
SW is a simple goroutine that scans one segment from a file and saves extracted data to the storage.
It reports any errors to the WP.

*/

// SegmentsIngest manages segments of data sources,
// discovers and saves to the index
type SegmentsIngest struct {
	storage *storage.Storage
	indexer *indexer.Indexer
	// segmentSize defines indexable unit in bytes
	segmentSize int64
	// concurrentSegments defines how many segments are indexed concurrently
	// indexing is CPU-bound and can affect other software in the system
	concurrentSegments int
}

type ingestTask struct {
	loc      common.Location
	filePath string
}
type ingestResult struct {
	ingestTask
	err       error
	terms     []string
	segment   common.IndexedSegment
	timeTaken time.Duration
}

type shutdown struct {
	context.Context
	cancel context.CancelFunc
}

func NewIngestor(
	storage *storage.Storage,
	indexer *indexer.Indexer,
	segmentSize int64,
	concurrentSegments int,
) *SegmentsIngest {
	if concurrentSegments < 1 {
		log.Fatalf("set more concurrentSegments")
	}

	return &SegmentsIngest{
		storage:            storage,
		indexer:            indexer,
		segmentSize:        segmentSize,
		concurrentSegments: concurrentSegments,
	}
}

// consumeResults loops through the results-channel and saves extracted data to the storage.
// it detects errors and propagates to the outer channel.
// returns two channels: incoming results for analyzing, failed results for propagating.
func (ingest *SegmentsIngest) consumeResults(results <-chan ingestResult, failedResults chan<- ingestResult, stop shutdown) error {

	// consuming results is IO-intensive as it composes an inverted index file.
	// good idea would be to parallel this as well as indexing segments.

	workers := ingest.concurrentSegments

	var wg errgroup.Group
	for i := 0; i < workers; i++ {
		wg.Go(func() error {
			for r := range results {
				if r.err != nil {
					err := xerrors.Errorf("segment ingestion failed: %w", r.err)
					log.Print(err)
					select {
					case failedResults <- r: // propagate
					default: // or just discard
					}
					continue
				}

				// The result is a success, so we save it to the storage below:
				t := time.Now()
				_, err := ingest.storage.CheckInSegment(r.segment, r.terms)
				if err != nil {
					err = xerrors.Errorf("segment save failed (%s[%d:%d]): %w", r.filePath, r.segment.Loc().Min, r.segment.Loc().Max, err)
					stop.cancel() // shut-down the rest
					return err
				}
				checkInTime := time.Now().Sub(t)

				log.Printf(
					"indexed %s[%d-%d](%d msgs) in %s",
					r.filePath,
					r.segment.Loc().Min,
					r.segment.Loc().Max,
					len(r.segment.Messages),
					(r.timeTaken + checkInTime).String(),
				)
			}
			return nil
		})
	}

	return wg.Wait()
}

// produceTasks produces all unindexed segments to the channel at the end it closes it.
// it listens for failed results on failedTasks, so it can skip inaccessible files.
// returns 2 channels: read-only with new tasks, write-only for accepting failed tasks.
func (ingest *SegmentsIngest) produceTasks(stop shutdown) (<-chan ingestTask, chan<- ingestResult) {
	newTasks := make(chan ingestTask)

	// this channel is used outside, so I won't close it, it'll be GCed after the shut-down.
	failedResults := make(chan ingestResult)
	bf := &blacklistedFiles{}
	go func() {
		// inspect failures and stop emitting tasks for "bad" files.
		// Hopefully next time the file will be blacklisted.
		for r := range failedResults {
			bf.add(r.filePath)
		}
	}()

	// start the loop in a go-routine:
	go func() {
		defer close(newTasks)

		// discover all files
		files, err := ingest.storage.AllFiles()
		if err != nil {
			err = xerrors.Errorf("unable to read data sources: %w", err)
			log.Print(err)
		}

		// assuming mostly one log file are being appended (the current file)
		// here we index file-by-file.
	filesLoop:
		for file, _ := range files {
			locs, err := ingest.storage.SelectLocationsForIndexing(file, ingest.segmentSize) // todo use composition
			if err != nil {
				err = xerrors.Errorf("unable to select unindexed segments: %w", err)
				log.Print(err)
				continue
			}
			for _, loc := range locs {
				if bf.test(file) {
					continue filesLoop
				}

				select {
				case newTasks <- ingestTask{loc, file}:
				case <-stop.Done():
					return // shut-down
				}
			}
		}
	}()

	return newTasks, failedResults
}

// workerPool (WP) accepts tasks and spreads them to workers goroutines (SW).
// it must wait until all workers are finished and handle all the results before returning.
func (ingest *SegmentsIngest) workerPool(tasks <-chan ingestTask, stop shutdown) <-chan ingestResult {
	results := make(chan ingestResult)

	streamPool := common.NewStreamsPool(ingest.concurrentSegments)
	defer streamPool.Close()

	// pool-control goroutine
	go func() {
		defer close(results)

		wg := sync.WaitGroup{}
		wg.Add(ingest.concurrentSegments)
		for i := 0; i < ingest.concurrentSegments; i++ {
			go func() {
				defer wg.Done()

				// wait for signals:
				var t ingestTask
				var ok bool

				for {
					select {
					case t, ok = <-tasks:
						if !ok {
							return
						}
					case <-stop.Done():
						return
					}

					start := time.Now()

					// process task
					fileStream, err := streamPool.Get(t.filePath)
					if err != nil {
						results <- ingestResult{t, err, nil, common.IndexedSegment{}, 0}
						return
					}
					defer streamPool.Put(fileStream)

					_, err = fileStream.Seek(t.loc.Min, io.SeekStart)
					if err != nil {
						err = xerrors.Errorf("unable to seek data source %s: %w", t.filePath, err)
						log.Print(err)
						return
					}

					// do the work, index the segment
					segment, terms, err := ingest.indexer.IndexSegment(fileStream, t.loc)
					segment.DataSource = common.HashFile(t.filePath)
					if err != nil {
						// this case is important as some segments may contain no messages at all, and thus should be ignored.
						// this is possible for long messages in the Stream, because we slice Stream into segments with no knowledge
						// about message sizes within.
						if errors.Is(err, scanner.NoMessageStartFound) {
							// todo: compare this and below
							continue
						}

						err = xerrors.Errorf("segment indexing failed in %s: %w", t.filePath, err)
						log.Print(err)
						results <- ingestResult{t, err, nil, common.IndexedSegment{}, 0}
						return
					}

					if len(segment.Messages) == 0 {
						// that means the given segment did not have any messages at all
						// todo: compare this and above
						continue
					}

					finish := time.Now().Sub(start)
					results <- ingestResult{t, nil, terms, segment, finish}
				}
			}()
		}
		wg.Wait()
	}()

	return results
}

// Ingest all unindexed segments across all known data sources.
// it parallels indexing and saving results to the storage.
// it does one pass.
func (ingest *SegmentsIngest) Ingest() (err error) {
	// stop allows any of the go-routines to shut-down the whole ingestion process.
	// the reasons must be solid for that decision.
	ctx, cancel := context.WithCancel(context.Background())
	stop := shutdown{ctx, cancel}
	defer cancel()

	// background processes:
	newTasks, reportFailedResults := ingest.produceTasks(stop)
	results := ingest.workerPool(newTasks, stop)

	// sync process:
	err = ingest.consumeResults(results, reportFailedResults, stop)
	if err != nil {
		err = xerrors.Errorf("consume ingestion results fail: %w", err)
	}

	return err
}

func (ingest *SegmentsIngest) GetSegmentSize() int64 {
	return ingest.segmentSize
}

// RescanTailMessages
// This is to fix the "partial tail message" problem (when indexing happens faster that appending).
// Rescan all messages that have unindexed areas ahead:
// [msg1]<non indexed area>[msg2]
func (ingest *SegmentsIngest) RescanTailMessages() error {

	var (
		filePath string
	)

	rescanMessage := func(filePath string, message common.IndexedMessage) (isTail bool, err error) {
		fileStream, err := os.Open(filePath)
		if err != nil {
			return
		}
		defer fileStream.Close()

		tailSegment, terms, err := ingest.indexer.IndexSegment(fileStream, message.Loc)
		if err != nil {
			err = xerrors.Errorf("index tail segment fail: %w", err)
			return
		}

		if len(tailSegment.Messages) == 0 {
			err = xerrors.Errorf("tail: rescan returned no messages: %v", message)
			return
		}

		newLoc := tailSegment.Messages[0].Loc
		if newLoc.Max == message.Loc.Max {
			// the message did not change, so a no-op
			return
		}
		isTail = tailSegment.Messages[0].IsTail // maybe the message is still a tail

		err = ingest.storage.UpdateTailMessage(int(message.Id), newLoc, terms)
		if err != nil {
			err = xerrors.Errorf("update tail segment fail: %w", err)
		}
		return
	}

	// Find all messages that are possibly fragmented
	r, err := ingest.storage.GetDb().Query(`
	SELECT m.id,m.posFrom,m.posTo,f.path
	FROM file_segments_messages m
	JOIN file_segments s ON m.segment_id=s.id
	JOIN files f ON f.fileHash=s.fileHash
	JOIN file_segments_messages_tail mt ON m.id=mt.message_id
	WHERE m.posTo<f.size
`)
	if err != nil {
		return xerrors.Errorf("tail: get messages fail: %w", err)
	}
	defer r.Close()
	for r.Next() {
		m := common.IndexedMessage{}
		err = r.Scan(&m.Id, &m.Loc.Min, &m.Loc.Max, &filePath)
		if err != nil {
			return xerrors.Errorf("tail: scan a message fail: %w", err)
		}

		// rescan
		isTail, err := rescanMessage(filePath, m)
		if err != nil {
			return xerrors.Errorf("tail: reindex a message fail: %w", err)
		}

		if isTail {
			// the message remains at the tail
			return nil
		}

		// drop the tail flag
		_, err = ingest.storage.GetDb().Exec("DELETE FROM file_segments_messages_tail WHERE message_id=?", m.Id)
		if err != nil {
			return xerrors.Errorf("tail: drop the flag: %w", err)
		}
	}

	return nil
}
