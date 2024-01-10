package indexer

import (
	"bytes"
	"errors"
	"golang.org/x/exp/maps"
	"golang.org/x/xerrors"
	"heaplog/common"
	"heaplog/scanner"
	"io"
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
func (indexer *Indexer) IndexSegment(
	stream io.ReadSeeker,
	location common.Location,
) (
	segment common.IndexedSegment,
	terms []string,
	err error,
) {
	_, err = stream.Seek(location.Min, io.SeekStart)
	if err != nil {
		return
	}

	termsMap := make(map[string]struct{})
	isMessageOutsideSegment := func(message *scanner.ScannedMessage) bool {
		effectivePos := location.Min + int64(message.Pos)
		return effectivePos >= location.Max
	}

	for m := range indexer.scanner.ScanMessagesCond(stream, isMessageOutsideSegment) {
		if m.Err != nil {
			if errors.Is(m.Err, io.EOF) {
				break
			}
			err = xerrors.Errorf("failed to scan a message: %w", m.Err)
			return
		}

		// do not index "date" of the message
		dateIndex := bytes.Index(m.Body, m.Date)
		indexableArea := append(m.Body[0:dateIndex], m.Body[dateIndex+len(m.Date):]...)
		messageTerms := indexer.tokenizer(string(indexableArea))

		for _, term := range messageTerms {
			termsMap[term] = struct{}{}
		}

		segment.Messages = append(segment.Messages, common.IndexedMessage{
			-1,
			common.Location{
				Min: location.Min + int64(m.Pos),
				Max: location.Min + int64(m.Pos) + int64(len(m.Body)),
			},
			m.DateTime,
			m.IsTail,
		})
	}

	terms = maps.Keys(termsMap)

	return
}
