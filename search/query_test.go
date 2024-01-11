package search

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"heaplog/common"
	"heaplog/test"
	"slices"
	"testing"
	"time"
)

//
// func TestQueryPerformance(t *testing.T) {
//
// 	storage, _, _ := test.PrepareServices(t)
//
// 	files := map[string]int64{
// 		"/home/dmitry/Code/go/src/heaplog2/local/logs/500Mb.log": 500_000_000,
// 	}
// 	_, _, err := storage.CheckInFiles(files)
// 	require.NoError(t, err)
//
// 	messageStartPattern := regexp.MustCompile(`(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`)
// 	dateLayout := "2006-01-02T15:04:05.000000-07:00"
// 	_scanner := scanner.NewScanner(dateLayout, messageStartPattern, 1_000_000, 50_000_000)
//
// 	tokenizerFunc := func(input string) []string {
// 		return tokenizer.TokenizeF(input, 4, 40)
// 	}
// 	_indexer := indexer.NewIndexer(_scanner, tokenizerFunc)
//
// 	ingestor := ingest.NewIngestor(storage, _indexer, 50_000_000, 10)
//
// 	require.NoError(t, ingestor.Ingest())
//
// 	// _, unboundTokenizerFunc := test.PrepareTokenizers()
// 	// _selector := NewSegmentSelector(storage, unboundTokenizerFunc, tokenizerFunc)
// 	// querySearch := NewQuerySearch(_selector, _selector.storage, _scanner)
// 	//
// 	// _, c, err := querySearch.NewQuery("error !debug", nil, nil, 100)
// 	// require.NoError(t, err)
// 	// <-c
//
// 	time.Sleep(5 * time.Second)
// }

func TestBuildQuerySuccess(t *testing.T) {
	selector, files := ingestFiles(t, 10)
	_scanner := test.PrepareScanner()
	filenames := maps.Keys(files)
	slices.Sort(filenames)

	// Message locations:
	// file1.log: {0,43}, {43,87}, {87,134}, {134,176}
	// file2.log: {0,42}, {42,82}, {82,135}, {135,177}

	querySearch := NewQuerySearch(selector, selector.storage, _scanner)

	type _test struct {
		query            string
		minDate, maxDate *time.Time
		expectedMessages []common.MatchedMessage
	}

	tests := []_test{
		{
			"message !first", // both terms address the same segments
			nil, nil,
			[]common.MatchedMessage{
				// file 1
				{-1, common.Location{43, 87}, test.MakeTimeV("2023-01-05 23:45:11.324153"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), "___", common.HashFile(filenames[0])},
				// file 2
				{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), "___", common.HashFile(filenames[1])},
			},
		},
		{
			"~.*", // full scan, all match
			nil, nil,
			[]common.MatchedMessage{
				// file 1
				{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{43, 87}, test.MakeTimeV("2023-01-05 23:45:11.324153"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), "___", common.HashFile(filenames[0])},
				// file 2
				{-1, common.Location{0, 42}, test.MakeTimeV("2023-02-01 01:00:00.000001"), "___", common.HashFile(filenames[1])},
				{-1, common.Location{42, 82}, test.MakeTimeV("2023-02-02 05:00:00.000002"), "___", common.HashFile(filenames[1])},
				{-1, common.Location{82, 135}, test.MakeTimeV("2023-02-03 10:00:00.000003"), "___", common.HashFile(filenames[1])},
				{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), "___", common.HashFile(filenames[1])},
			},
		},
		{
			"~(m|i)", // full scan, all match
			test.MakeTimeP("2023-01-05 23:46:22.234123"),
			test.MakeTimeP("2023-02-01 01:00:00.000001"),
			[]common.MatchedMessage{
				// file 1
				{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), "___", common.HashFile(filenames[0])},
				// file 2
				{-1, common.Location{0, 42}, test.MakeTimeV("2023-02-01 01:00:00.000001"), "___", common.HashFile(filenames[1])},
			},
		},
		{
			"~fir", // full scan
			nil, nil,
			[]common.MatchedMessage{
				// file 1
				{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), "___", common.HashFile(filenames[0])},
				// file 2
			},
		},
		{
			"~fif|fir", // full scan
			nil, nil,
			[]common.MatchedMessage{
				// file 1
				{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), "___", common.HashFile(filenames[0])},
				// file 2
				{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), "___", common.HashFile(filenames[1])},
			},
		},
		{
			"~(fif)", // full scan
			nil, nil,
			[]common.MatchedMessage{
				// file 1
				// file 2
				{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), "___", common.HashFile(filenames[1])},
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test%d: %s", i, tt.query), func(t *testing.T) {
			queryHash := hashQuery(tt.query, tt.minDate, tt.maxDate)

			_, rChan, err := querySearch.NewQuery(tt.query, tt.minDate, tt.maxDate, 1)
			require.NoError(t, err)

			r1 := <-rChan
			require.NoError(t, err)
			require.True(t, r1.FirstPageReady)
			require.False(t, r1.QueryComplete)

			r2 := <-rChan
			require.NoError(t, err)
			require.False(t, r2.FirstPageReady)
			require.True(t, r2.QueryComplete)

			time.Sleep(20 * time.Millisecond) // flush appenders

			actualMessages, err := selector.storage.GetMessagePage(queryHash, 1000, 0, nil, nil)
			require.NoError(t, err)
			require.EqualValues(t, len(tt.expectedMessages), len(actualMessages))

			for i, expectedMessage := range tt.expectedMessages {
				actualMessage := actualMessages[i]

				require.True(t, expectedMessage.Date.Equal(actualMessage.Date))
				require.Equal(t, expectedMessage.Loc, actualMessage.Loc)
			}

		})
	}
}

func TestQueryIdempotency(t *testing.T) {
	selector, files := ingestFiles(t, 10)
	_scanner := test.PrepareScanner()
	filenames := maps.Keys(files)
	slices.Sort(filenames)

	// Message locations:
	// file1.log: {0,43}, {43,87}, {87,134}, {134,176}
	// file2.log: {0,42}, {42,82}, {82,135}, {135,177}

	querySearch := NewQuerySearch(selector, selector.storage, _scanner)

	type _test struct {
		query            string
		minDate, maxDate *time.Time
		expectedMessages []common.MatchedMessage
	}

	tests := []_test{
		{
			"message !first", // both terms address the same segments
			nil, nil,
			[]common.MatchedMessage{
				// file 1
				{-1, common.Location{43, 87}, test.MakeTimeV("2023-01-05 23:45:11.324153"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), "___", common.HashFile(filenames[0])},
				{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), "___", common.HashFile(filenames[0])},
				// file 2
				{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), "___", common.HashFile(filenames[1])},
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test%d: %s", i, tt.query), func(t *testing.T) {
			queryHash := hashQuery(tt.query, tt.minDate, tt.maxDate)

			// make the same query multiple times:
			for i := 0; i < 2; i++ {
				_, rChan, err := querySearch.NewQuery(tt.query, tt.minDate, tt.maxDate, 1)
				require.NoError(t, err)
				r1 := <-rChan
				require.NoError(t, err)
				require.True(t, r1.FirstPageReady)
				require.False(t, r1.QueryComplete)
				r2 := <-rChan
				require.NoError(t, err)
				require.False(t, r2.FirstPageReady)
				require.True(t, r2.QueryComplete)

				time.Sleep(100 * time.Millisecond) // flush appenders
			}

			actualMessages, err := selector.storage.GetMessagePage(queryHash, 1000, 0, nil, nil)
			require.NoError(t, err)
			require.EqualValues(t, len(tt.expectedMessages), len(actualMessages))

			for i, expectedMessage := range tt.expectedMessages {
				actualMessage := actualMessages[i]

				require.True(t, expectedMessage.Date.Equal(actualMessage.Date))
				require.Equal(t, expectedMessage.Loc, actualMessage.Loc)
			}

		})
	}
}

func hashQuery(query string, minDate, maxDate *time.Time) string {
	expr, _ := ParseUserQuery(query)
	minDateMicro, maxDateMicro := int64(0), int64(0)
	if minDate != nil {
		minDateMicro = minDate.UnixMicro()
	}
	if maxDate != nil {
		maxDateMicro = maxDate.UnixMicro()
	}
	return common.HashString(fmt.Sprintf("%s%d%d", expr.Hash(), minDateMicro, maxDateMicro))
}
