package search

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	go_iterators "github.com/lezhnev74/go-iterators"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/test_util"
)

func TestBatching(t *testing.T) {
	src := []db.Message{
		{SegmentId: 1, Loc: common.Location{From: 1, To: 1}, FileId: 1},
		{SegmentId: 1, Loc: common.Location{From: 2, To: 2}, FileId: 1},
		{SegmentId: 2, Loc: common.Location{From: 3, To: 3}, FileId: 1},
		{SegmentId: 3, Loc: common.Location{From: 1, To: 1}, FileId: 2},
		{SegmentId: 4, Loc: common.Location{From: 2, To: 2}, FileId: 2},
	}

	// Group by file

	groupedIt := go_iterators.NewGroupingIterator(
		go_iterators.NewSliceIterator(src),
		func(m db.Message) any { return m.FileId },
	)
	expected := [][]db.Message{
		{
			{SegmentId: 1, Loc: common.Location{From: 1, To: 1}, FileId: 1},
			{SegmentId: 1, Loc: common.Location{From: 2, To: 2}, FileId: 1},
			{SegmentId: 2, Loc: common.Location{From: 3, To: 3}, FileId: 1},
		},
		{
			{SegmentId: 3, Loc: common.Location{From: 1, To: 1}, FileId: 2},
			{SegmentId: 4, Loc: common.Location{From: 2, To: 2}, FileId: 2},
		},
	}
	require.Equal(t, expected, go_iterators.ToSlice(groupedIt))

	// Group by segment
	groupedIt = go_iterators.NewGroupingIterator(
		go_iterators.NewSliceIterator(src),
		func(m db.Message) any { return m.SegmentId },
	)
	expected = [][]db.Message{
		{
			{SegmentId: 1, Loc: common.Location{From: 1, To: 1}, FileId: 1},
			{SegmentId: 1, Loc: common.Location{From: 2, To: 2}, FileId: 1},
		},
		{
			{SegmentId: 2, Loc: common.Location{From: 3, To: 3}, FileId: 1},
		},
		{
			{SegmentId: 3, Loc: common.Location{From: 1, To: 1}, FileId: 2},
		},
		{
			{SegmentId: 4, Loc: common.Location{From: 2, To: 2}, FileId: 2},
		},
	}
	require.Equal(t, expected, go_iterators.ToSlice(groupedIt))
}

func TestReadMatch(t *testing.T) {

	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	sampleFile := `
[2023-01-05T23:42:00.213212+00:00] testing.Info: message 
multile
	possibly
		very long
 [2023-01-05T23:45:00.213212+00:00] <- could include dates too
[2023-01-05T23:48:00.213212+00:00] testing.DEBUG: message 2
`
	file := test_util.PopulateFile(storageRoot, []byte(sampleFile))
	defer func() { _ = os.RemoveAll(storageRoot) }()

	type test struct {
		messages         []db.Message
		matcher          SearchMatcher
		expectedMessages []db.Message
		err              error
	}

	tests := []test{
		{ // MATCH ALL
			messages: []db.Message{
				{Loc: common.Location{From: 1, To: 152}, RelDateLoc: common.Location{From: 1, To: 32}},
				{Loc: common.Location{From: 152, To: 194}, RelDateLoc: common.Location{From: 1, To: 32}},
			},
			matcher: func(m db.Message, body []byte) bool { return true },
			expectedMessages: []db.Message{
				{Loc: common.Location{From: 1, To: 152}, RelDateLoc: common.Location{From: 1, To: 32}, Date: test_util.MakeTimeP("2023-01-05T23:42:00.213212+00:00")},
				{Loc: common.Location{From: 152, To: 194}, RelDateLoc: common.Location{From: 1, To: 32}, Date: test_util.MakeTimeP("2023-01-05T23:48:00.213212+00:00")},
			},
		},
		{ // MATCH NONE
			messages: []db.Message{
				{Loc: common.Location{From: 1, To: 152}, RelDateLoc: common.Location{From: 1, To: 32}},
				{Loc: common.Location{From: 152, To: 194}, RelDateLoc: common.Location{From: 1, To: 32}},
			},
			matcher:          func(m db.Message, body []byte) bool { return false },
			expectedMessages: nil,
		},
		{ // MATCH ONE
			messages: []db.Message{
				{Loc: common.Location{From: 1, To: 152}, RelDateLoc: common.Location{From: 1, To: 32}},
				{Loc: common.Location{From: 152, To: 194}, RelDateLoc: common.Location{From: 1, To: 32}},
			},
			matcher: func(m db.Message, body []byte) bool { return bytes.Contains(body, []byte("multile")) },
			expectedMessages: []db.Message{
				{Loc: common.Location{From: 1, To: 152}, RelDateLoc: common.Location{From: 1, To: 32}, Date: test_util.MakeTimeP("2023-01-05T23:42:00.213212+00:00")},
			},
		},
		//{ // ERROR: READ OUT OF BOUND
		//	messages: []db.Message{
		//		{Loc: common.Location{500, 600}},
		//	},
		//	matcher: func(m db.Message, body []byte) bool { return true },
		//	expectedMessages: []db.Message{
		//		{Loc: common.Location{500, 600}},
		//	},
		//	err: io.EOF,
		//},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			matchedIt, err := StreamFileMatch(file, tt.messages, tt.matcher, "2006-01-02T15:04:05.000000-07:00")
			require.NoError(t, err)

			var matchedMessages []db.Message
			for ev := range matchedIt {
				if ev.Err != nil {
					if errors.Is(err, go_iterators.EmptyIterator) {
						break
					}
					require.ErrorIs(t, ev.Err, tt.err)
				}
				matchedMessages = append(matchedMessages, ev.Val)
			}

			require.Equal(t, tt.expectedMessages, matchedMessages)
		})
	}
}
