package search

import (
	"context"
	"fmt"
	"iter"
	"os"
	"time"

	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

type ReadableIndex interface {
	// GetSegments uses Inverted Index to get potential segments
	GetSegments(terms [][]byte) ([]int, error)
	// GetMessages streams all messages within given segments
	GetMessages(segments []int, minDate, maxDate *time.Time) (iter.Seq[common.FileMessage], error)
}

type Search struct {
	ctx      context.Context
	tokenize func([]byte) [][]byte
	index    ReadableIndex
	logger   *zap.Logger
}

// Search is the main gateway to the message-matching functionality.
// Given the user query expression, it decides if the inverted index can be used
// to reduce the amount of messages to test.
// It streams out matched messages.
func (s *Search) Search(expr *query_language.Expression, minDate, maxDate *time.Time) (
	found iter.Seq2[common.Message, []byte],
	err error,
) {
	//exprMatcher := expr.GetMatcher()
	//matcher := func(m common.Message, body []byte) bool {
	//	// exclude date from matching
	//	pos := func(pos int) int { return pos - m.Loc.From }
	//	body = append(body[:pos(m.DateLoc.From)], body[pos(m.DateLoc.To):]...)
	//	bodyString := unsafe.String(unsafe.SliceData(body), len(body))
	//	result := exprMatcher(query_language.NewCachedString(bodyString))
	//	return result
	//}
	//
	//segments := []int(nil) // segments to look into for messages (nil = All)
	//if !ShouldFullScan(expr, s.tokenize) {
	//	terms := make([][]byte, 0)
	//	for _, t := range expr.FindKeywords() {
	//		terms = append(terms, []byte(t))
	//	}
	//	segments, err = s.index.GetSegments(terms)
	//	if err != nil {
	//		err = fmt.Errorf("get segments by terms: %w", err)
	//		return
	//	}
	//	s.logger.Debug("Selected segments: %d\n", zap.Int("len", len(segments)))
	//}
	//
	//_, err = s.index.GetMessages(segments, minDate, maxDate)
	//if err != nil {
	//	err = fmt.Errorf("get messages: %w", err)
	//	return
	//}

	//messagesBodies, err := StreamFileMatch(fileMessages, matcher)
	//
	//return func(yield func(common.Message, []byte) bool) {
	//	for m := range fileMessages {
	//		if !matcher(m, b) {
	//			continue
	//		}
	//		if !yield(m, b) {
	//			break
	//		}
	//	}
	//}, nil

	return
}

func ReadMessages(messages iter.Seq[common.FileMessage]) iter.Seq2[common.FileMessageBody, error] {
	var (
		stream *os.File
		err    error
	)
	return func(yield func(common.FileMessageBody, error) bool) {
		for m := range messages {
			if stream == nil || stream.Name() != m.File {
				stream, err = os.Open(m.File)
				if err != nil {
					yield(common.FileMessageBody{}, fmt.Errorf("file open %s: %w", m.File, err))
					return
				}
			}

			mLen := m.Loc.To - m.Loc.From
			buf := make([]byte, mLen) // alloc memory for the message
			_, err = stream.ReadAt(buf, int64(m.Loc.From))
			if err != nil {
				yield(common.FileMessageBody{}, fmt.Errorf("read file %s: %w", m.File, err))
				return
			}

			if !yield(common.FileMessageBody{FileMessage: m, Body: buf}, nil) {
				return
			}
		}
	}
}
