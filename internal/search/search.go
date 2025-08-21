package search

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"slices"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

type ReadableIndex interface {
	// GetRelevantSegments uses Inverted Index to get potential segments
	GetRelevantSegments(terms [][]byte) (map[string][]int, error)
	// GetMessages streams all messages within given segments
	GetMessages(segments []int, minDate, maxDate *time.Time) (iter.Seq[common.FileMessage], error)
}

type Search struct {
	ctx      context.Context
	tokenize func([]byte) [][]byte
	index    ReadableIndex
	logger   *zap.Logger
}

func NewSearch(ctx context.Context, tokenize func([]byte) [][]byte, index ReadableIndex, logger *zap.Logger) *Search {
	return &Search{
		ctx:      ctx,
		tokenize: tokenize,
		index:    index,
		logger:   logger,
	}
}

// Search is the main gateway to the message-matching functionality.
// Given the user query expression, it decides if the inverted index can be used
// to reduce the amount of messages to test.
// It streams out matched messages.
func (s *Search) Search(expr *query_language.Expression, minDate, maxDate *time.Time) (
	iter.Seq[common.FileMessageBody],
	error,
) {
	exprMatcher := expr.GetMatcher()
	matcher := func(m common.FileMessageBody) bool {
		// exclude date from matching in the separate buffer
		pos := func(pos int) int { return pos - m.Loc.From }
		body := append([]byte{}, m.Body[:pos(m.DateLoc.From)]...)
		body = append(body, m.Body[pos(m.DateLoc.To):]...)
		bodyString := unsafe.String(unsafe.SliceData(body), len(body))
		result := exprMatcher(query_language.NewCachedString(bodyString))
		return result
	}

	segments := []int(nil) // segments to look into for messages (nil = All)
	if !shouldFullScan(expr, s.tokenize) {
		terms := make([][]byte, 0)
		for _, t := range expr.FindKeywords() {
			terms = append(terms, s.tokenize([]byte(t))...)
		}
		slices.SortFunc(terms, bytes.Compare)
		slices.CompactFunc(terms, bytes.Equal)

		termSegments, err := s.index.GetRelevantSegments(terms)
		if err != nil {
			return nil, fmt.Errorf("get segments by terms: %w", err)
		}

		setsExpr := exprMapLiteralsToSets(expr, s.tokenize, termSegments)
		segments = exprEval(setsExpr)
		if slices.Equal(segments, allSegmentsSuperset) {
			segments = nil // full-scan
		} else if len(segments) == 0 {
			// not a full-scan, but no relevant segments found in II, so early return
			s.logger.Debug("No relevant segments found for the query", zap.String("query", expr.String()))
			return common.Empty[common.FileMessageBody](), nil
		}
	}
	if len(segments) > 0 {
		s.logger.Debug("Selected segments\n", zap.Int("len", len(segments)), zap.String("query", expr.String()))
	}

	fileMessages, err := s.index.GetMessages(segments, minDate, maxDate)
	if err != nil {
		return nil, fmt.Errorf("get messages: %w", err)
	}

	return func(yield func(body common.FileMessageBody) bool) {
		for mfb := range common.ReadMessages(s.ctx, fileMessages) {
			if !matcher(mfb) {
				continue
			}
			if !yield(mfb) {
				break
			}
		}
	}, nil
}
