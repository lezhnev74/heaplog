package search_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/query_language"
	"heaplog_2024/search"
	"heaplog_2024/test_util"
	"heaplog_2024/tokenizer"
)

func TestSearchResults(t *testing.T) {

	_db, storageRoot := test_util.PrepareTestDb(t)
	defer func() { _ = os.RemoveAll(storageRoot) }()
	files := PrepareTestFiles(t, storageRoot)

	_, _, err := _db.CheckInFiles(files)
	require.NoError(t, err)

	file1Id, _ := _db.GetFileId(files[0])
	file2Id, _ := _db.GetFileId(files[1])

	ing, ii := test_util.PrepareTestIngest(t, 50, storageRoot, _db)
	err = ing.Index(files)
	require.NoError(t, err)

	_db.MessagesDb.Flush()

	dateFormat := "2006-01-02T15:04:05.000000-07:00"
	tokenize := func(s []byte) [][]byte {
		return tokenizer.Tokenize(s, 4, 8)
	}
	s := search.NewSearch(context.Background(), _db, ii, dateFormat)

	type test struct {
		query           string
		isFullScan      bool
		matchedMessages []db.Message
	}

	tests := []test{
		{ // one short term
			query:      "err",
			isFullScan: true,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 1, To: 64}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:00:10.100160+00:00")},
				{Loc: common.Location{From: 197, To: 258}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:03:30.449222+00:00")},
				{Loc: common.Location{From: 71, To: 130}, FileId: file2Id, Date: test_util.MakeTimeP("2024-08-01T00:02:02.967490+00:00")},
			},
		},
		{ // few short terms
			query:      "err car",
			isFullScan: true,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 1, To: 64}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:00:10.100160+00:00")},
			},
		},
		{ // FULL-SCAN (re)
			query:      "~err",
			isFullScan: true,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 1, To: 64}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:00:10.100160+00:00")},
				{Loc: common.Location{From: 197, To: 258}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:03:30.449222+00:00")},
				{Loc: common.Location{From: 71, To: 130}, FileId: file2Id, Date: test_util.MakeTimeP("2024-08-01T00:02:02.967490+00:00")},
			},
		},
		{ // FULL-SCAN (re)
			query:      `~userid:\d+`,
			isFullScan: true,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 64, To: 135}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:01:10.100170+00:00")},
				{Loc: common.Location{From: 1, To: 71}, FileId: file2Id, Date: test_util.MakeTimeP("2024-08-01T00:01:01.285087+00:00")},
			},
		},
		//{ // case-sensitive match
		//	query:           "Event",
		//	isFullScan:      false,
		//	matchedMessages: []db.Message{},
		//},
		{ // case-sensitive match
			query:      "Event",
			isFullScan: false,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 64, To: 135}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:01:10.100170+00:00")},
				{Loc: common.Location{From: 1, To: 71}, FileId: file2Id, Date: test_util.MakeTimeP("2024-08-01T00:01:01.285087+00:00")},
			},
		},
		{ // two terms
			query:      "event signup",
			isFullScan: false,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 64, To: 135}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:01:10.100170+00:00")},
			},
		},
		{ // two terms with inversion
			query:      "event !login",
			isFullScan: false,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 64, To: 135}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:01:10.100170+00:00")},
			},
		},
		{ // two terms OR
			query:      "error or failure",
			isFullScan: false,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 1, To: 64}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:00:10.100160+00:00")},
				{Loc: common.Location{From: 135, To: 197}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:02:10.383227+00:00")},
				{Loc: common.Location{From: 197, To: 258}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:03:30.449222+00:00")},
				{Loc: common.Location{From: 71, To: 130}, FileId: file2Id, Date: test_util.MakeTimeP("2024-08-01T00:02:02.967490+00:00")},
			},
		},
		{ // short and long terms
			query:      "api failure",
			isFullScan: false,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 135, To: 197}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:02:10.383227+00:00")},
			},
		},
		{ // two not
			query:      "!~err !~fail",
			isFullScan: true,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 64, To: 135}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:01:10.100170+00:00")},
				{Loc: common.Location{From: 258, To: 310}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:04:20.082156+00:00")},
				{Loc: common.Location{From: 1, To: 71}, FileId: file2Id, Date: test_util.MakeTimeP("2024-08-01T00:01:01.285087+00:00")},
			},
		},
		{ // compound literal
			query:      `"payment accepted"`,
			isFullScan: false,
			matchedMessages: []db.Message{
				{Loc: common.Location{From: 258, To: 310}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:04:20.082156+00:00")},
			},
		},
		{ // compound literal must be exact
			query:           `"accepted payment"`,
			isFullScan:      false,
			matchedMessages: []db.Message{},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d: %s", i, tt.query), func(t *testing.T) {
			expr, err := query_language.ParseUserQuery(tt.query)
			require.NoError(t, err)
			it, isFullScan, err := s.Search(expr, nil, nil, dateFormat, tokenize, 1)
			require.NoError(t, err)
			require.Equal(t, tt.isFullScan, isFullScan)
			messages := go_iterators.ToSlice(it)

			require.Equal(t, len(tt.matchedMessages), len(messages))
			for i, mm := range messages {
				require.Equal(t, tt.matchedMessages[i].FileId, mm.FileId)
				require.Equal(t, tt.matchedMessages[i].Loc, mm.Loc)
				require.Equal(t, tt.matchedMessages[i].Date, mm.Date)
			}
		})
	}
}

// PrepareTestFiles generates static files with messages.
// This files are used in e2e tests for ingestion/searching/query evaluation flow.
func PrepareTestFiles(t *testing.T, storageRoot string) (files []string) {
	// Populate files for tests
	sampleFile1 := `
[2024-07-30T00:00:10.100160+00:00] payment error: invalid card
[2024-07-30T00:01:10.100170+00:00] event triggered: signup (userid:12)
[2024-07-30T00:02:10.383227+00:00] api failure: Quota reached
[2024-07-30T00:03:30.449222+00:00] file error: no disk space
[2024-07-30T00:04:20.082156+00:00] payment accepted
`
	sampleFile2 := `
[2024-08-01T00:01:01.285087+00:00] event triggered: login (userid:39)
[2024-08-01T00:02:02.967490+00:00] payment error: no funds
`
	/*
		// FILE 1:
		{Loc: common.Location{1, 64}, FileId: file1Id},
		{Loc: common.Location{64, 135}, FileId: file1Id},
		{Loc: common.Location{135, 197}, FileId: file1Id},
		{Loc: common.Location{197, 258}, FileId: file1Id},
		{Loc: common.Location{258, 310}, FileId: file1Id},
		// FILE 2
		{Loc: common.Location{1, 71}, FileId: file2Id},
		{Loc: common.Location{71, 130}, FileId: file2Id},
	*/

	files = []string{
		test_util.PopulateFile(storageRoot, []byte(sampleFile1)),
		test_util.PopulateFile(storageRoot, []byte(sampleFile2)),
	}

	return
}

func TestFullScan(t *testing.T) {
	_db, storageRoot := test_util.PrepareTestDb(t)
	defer func() { _ = os.RemoveAll(storageRoot) }()

	// Populate files for tests
	sampleFile1 := `
[2024-07-30T00:00:04.000000+00:00] testing.Info: message
	multiline
[2024-07-30T00:00:05.111111+00:00] testing.DEBUG: message 2
`
	sampleFile2 := `
[2024-07-30T00:00:06.222222+00:00] testing.Info: message 3
multile
	possibly
		very long
 [2024-07-30T00:00:07.333333+00:00] <- could include dates too
[2024-07-30T00:00:08.444444+00:00] testing.DEBUG: message 4
`

	file1 := test_util.PopulateFile(storageRoot, []byte(sampleFile1))
	file2 := test_util.PopulateFile(storageRoot, []byte(sampleFile2))

	// Ingest data before testing search
	ing, ii := test_util.PrepareTestIngest(t, 50, storageRoot, _db)
	s := search.NewSearch(context.Background(), _db, ii, "2006-01-02T15:04:05.000000-07:00")

	_, _, err := _db.CheckInFiles([]string{file1, file2})
	require.NoError(t, err)

	file1Id, _ := _db.GetFileId(file1)
	file2Id, _ := _db.GetFileId(file2)

	err = ing.Index([]string{file1, file2})
	require.NoError(t, err)

	_db.MessagesDb.Flush()

	type test struct {
		matcher          search.SearchMatcher
		expectedMessages []db.Message
		err              error
	}

	tests := []test{
		{ // MATCH ALL
			matcher: func(m db.Message, body []byte) bool { return true },
			expectedMessages: []db.Message{
				{SegmentId: 1, Loc: common.Location{From: 1, To: 69}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:00:04.000000+00:00")},
				{SegmentId: 2, Loc: common.Location{From: 69, To: 129}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file1Id, Date: test_util.MakeTimeP("2024-07-30T00:00:05.111111+00:00")},
				{SegmentId: 3, Loc: common.Location{From: 1, To: 153}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id, Date: test_util.MakeTimeP("2024-07-30T00:00:06.222222+00:00")},
				{SegmentId: 4, Loc: common.Location{From: 153, To: 213}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id, Date: test_util.MakeTimeP("2024-07-30T00:00:08.444444+00:00")},
			},
		},
		{ // MATCH NONE
			matcher:          func(m db.Message, body []byte) bool { return false },
			expectedMessages: nil,
		},
		{ // MATCH ONE
			matcher: func(m db.Message, body []byte) bool { return bytes.Contains(body, []byte("multile")) },
			expectedMessages: []db.Message{
				{SegmentId: 3, Loc: common.Location{From: 1, To: 153}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id, Date: test_util.MakeTimeP("2024-07-30T00:00:06.222222+00:00")},
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			matchedIt, err := s.FullScan(tt.matcher)
			require.NoError(t, err)

			var matchedMessages []db.Message
			for {
				m, err := matchedIt.Next()
				if err != nil {
					if errors.Is(err, go_iterators.EmptyIterator) {
						break
					}
					require.ErrorIs(t, err, tt.err)
				}
				matchedMessages = append(matchedMessages, m)
			}

			require.Equal(t, tt.expectedMessages, matchedMessages)
		})
	}
}
