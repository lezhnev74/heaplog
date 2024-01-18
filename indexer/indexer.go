package indexer

import (
	"errors"
	"golang.org/x/exp/maps"
	"golang.org/x/xerrors"
	"heaplog/common"
	"heaplog/scanner"
	"io"
	"unsafe"
)

// Indexer is a stateless service that indexes a given source segment
// and saves data to the storage via a 2-phase commit
type Indexer struct {
	scanner   *scanner.Scanner
	tokenizer func(string) []string
}

func NewIndexer(scanner *scanner.Scanner, tokenizer func(string) []string) *Indexer {
	return &Indexer{
		scanner:   scanner,
		tokenizer: tokenizer,
	}
}

// IndexSegment analyzes all messages within one location in the stream
// It returns the indexed segment aligned to message boundaries (even if extends originally given location)
// Location is related to the stream.
func (indexer *Indexer) IndexSegment(stream io.ReadSeeker, location common.Location) (
	segment common.IndexedSegment,
	terms []string,
	err error,
) {
	_, err = stream.Seek(location.Min, io.SeekStart)
	if err != nil {
		return
	}

	termsMap := make(map[string]struct{})
	onEachMessage := func(m *scanner.ScannedMessage) bool {
		effectivePos := location.Min + int64(m.Pos)
		if effectivePos >= location.Max {
			return true // stop iteration
		}

		// do not index "date" of the message
		// Index "before the date" area
		beforeDate := unsafe.String(unsafe.SliceData(m.Body[:m.DateFrom]), m.DateFrom)
		messageTerms := indexer.tokenizer(beforeDate)
		for _, term := range messageTerms {
			termsMap[term] = struct{}{}
		}

		// Index "after the date" area
		afterDate := unsafe.String(unsafe.SliceData(m.Body[m.DateTo:]), m.Len-m.DateTo)
		messageTerms = indexer.tokenizer(afterDate)
		for _, term := range messageTerms {
			termsMap[term] = struct{}{}
		}

		segment.Messages = append(segment.Messages, common.IndexedMessage{
			-1, // no id during indexing
			common.Location{
				Min: location.Min + int64(m.Pos),
				Max: location.Min + int64(m.Pos) + int64(len(m.Body)),
			},
			m.DateTime,
			m.IsTail,
		})

		return false
	}

	err = indexer.scanner.Scan(stream, onEachMessage)
	if err != nil && !errors.Is(err, io.EOF) {
		err = xerrors.Errorf("failed to scan a message: %w", err)
		return
	}

	terms = maps.Keys(termsMap)

	return
}
