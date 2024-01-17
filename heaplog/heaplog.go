package heaplog

import (
	"golang.org/x/xerrors"
	"heaplog/common"
	"heaplog/indexer"
	"heaplog/ingest"
	"heaplog/scanner"
	"heaplog/search"
	"heaplog/storage"
	"log"
	"regexp"
	"time"
)

// Heaplog is the central controller of the app.
// It exposes use-cases that can be integrated with IO channels (console, web, etc.).
type Heaplog struct {
	storage  *storage.Storage
	ingestor *ingest.SegmentsIngest
	discover *ingest.Discover
	search   *search.QuerySearch
}

func NewHeaplog(
	// directory where internal state is stored (indexed data)
	storageRoot string,
	messageStartPattern *regexp.Regexp,
	dateLayout string,
	globs []string,
	ingestFlushTick, searchFlushTick time.Duration, // its part
	tokenizerFunc func(input string) []string,
	unboundTokenizerFunc func(input string) []string,
	indexSegmentSize int64,
	ingestWorkers int,
) (*Heaplog, error) {
	s, err := storage.NewStorage(storageRoot, ingestFlushTick, searchFlushTick)
	if err != nil {
		err = xerrors.Errorf("storage init failed: %w", err)
		return nil, err
	}

	_scanner := scanner.NewScanner(dateLayout, messageStartPattern, 10_000_000, 100_000_000)
	_indexer := indexer.NewIndexer(_scanner, tokenizerFunc)
	ingestor := ingest.NewIngestor(s, _indexer, indexSegmentSize, ingestWorkers)
	discover := ingest.NewDiscover(globs, s)

	_selector := search.NewSegmentSelector(s, unboundTokenizerFunc, tokenizerFunc)
	_search := search.NewQuerySearch(_selector, s, _scanner)

	hl := &Heaplog{
		storage:  s,
		ingestor: ingestor,
		discover: discover,
		search:   _search,
	}

	return hl, nil
}

// Stats return basic information about the service's state
// todo
func (h *Heaplog) Stats() (map[string]string, error) {
	ret := make(map[string]string)

	return ret, nil
}

// NewQuery builds results for the query and returns the query identifier as soon as one page of data is available.
// Reading results is supposed to be done later via h.QueryPage()
func (h *Heaplog) NewQuery(text string, pageSize int, from *time.Time, to *time.Time) (queryId string, err error) {
	queryId, resultCh, err := h.search.NewQuery(text, from, to, pageSize)
	if err != nil {
		err = xerrors.Errorf("new query: %w", err)
		return
	}

	for qr := range resultCh {
		if qr.Err != nil {
			err = xerrors.Errorf("new query failed: %w", qr.Err)
			return
		}

		if qr.FirstPageReady {
			return
		}
	}

	// we get here if the query finds less than a page of data (possibly empty)
	return
}

func (h *Heaplog) QuerySummary(id string, from, to *time.Time) (common.QuerySummary, error) {
	summary, err := h.storage.GetQuerySummary(id, from, to)
	if err != nil {
		err = xerrors.Errorf("query summary failed: %w", err)
	}
	return summary, err
}

func (h *Heaplog) AllQueriesSummaries() ([]common.QuerySummary, error) {
	summaries, err := h.storage.GetQueriesSummaries()
	if err != nil {
		err = xerrors.Errorf("query summaries failed: %w", err)
	}
	return summaries, err
}

func (h *Heaplog) QueryPage(id string, page, pageSize int, from, to *time.Time) ([][]byte, error) {
	messageLocations, err := h.storage.GetMessagePage(id, pageSize, page, from, to)
	if err != nil {
		err = xerrors.Errorf("query page failed: %w", err)
		return nil, err
	}

	messages, err := h.search.ReadMessages(messageLocations)
	if err != nil {
		err = xerrors.Errorf("query page failed: %w", err)
		return nil, err
	}

	return messages, nil
}

// Background starts service background workers.
// This is part of the boot-up process.
func (h *Heaplog) Background() {
	go h.loopIngest()
	go h.SegmentsCleanup()
}

func (h *Heaplog) SegmentsCleanup() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

StopLoop:
	for {
		select {
		case <-t.C:
			// Correct tail messages first, then merge (as both operations modify the same segment records)
			err := h.ingestor.RescanTailMessages()
			if err != nil {
				err = xerrors.Errorf("rescan tail segment fail: %w", err)
				break StopLoop
			}

			_, err = h.storage.MergeSegments(h.ingestor.GetSegmentSize())
			if err != nil {
				log.Printf("merging segments failed: %s", err)
				break StopLoop
			}

			err = h.storage.EvictQueries()
			if err != nil {
				log.Printf("evicting queries failed: %s", err)
				break StopLoop
			}
		}
	}

}

func (h *Heaplog) AggregatePage(queryId string, discretization int, from, to time.Time) (a map[int64]int64, err error) {

	curUnit := calculateDiscreteUnit(from, to, discretization)
	a, err = h.storage.QueryAggregate(queryId, curUnit, from, to)
	if err != nil {
		err = xerrors.Errorf("aggregating query failed: %w", err)
	}

	// try to show more precise timeline even if theoretically data won't fit into discretization window
	units := []string{"year", "month", "day", "hour", "minute", "second", "millisecond"}
	var check bool
	for _, unit := range units {
		if unit == curUnit {
			check = true
			continue
		}
		if !check {
			continue
		}

		curMap, err := h.storage.QueryAggregate(queryId, unit, from, to)
		if err != nil {
			err = xerrors.Errorf("aggregating query failed: %w", err)
		}
		if len(curMap) > discretization {
			break // too much data
		}
		a = curMap
	}

	return
}

// loopIngest indexes new files (or new parts of old files)
func (h *Heaplog) loopIngest() {
	for {
		err := h.discover.DiscoverFiles()
		if err != nil {
			log.Printf("loop discover failed: %+v", err)
			break
		}

		err = h.ingestor.Ingest()
		if err != nil {
			log.Printf("loop ingest failed: %+v", err)
			break
		}

		time.Sleep(60 * time.Second)
	}
}

// calculateDiscreteUnit selects appropriate aggregation precision To fit in the discreteCount size
func calculateDiscreteUnit(from time.Time, to time.Time, discreteCount int) string {

	factors := []struct {
		factor float64
		unit   string
	}{
		{1_000, "second"},
		{60, "minute"},
		{60, "hour"},
		{24, "day"},
		{30.5, "month"},
		{12, "year"},
	}

	points := float64(to.Sub(from).Milliseconds())
	unit := "millisecond"
	for _, f := range factors {
		if points < float64(discreteCount) {
			return unit
		}
		points = points / f.factor
		unit = f.unit
	}
	return unit
}
