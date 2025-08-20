package internal

import (
	"context"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/lezhnev74/inverted_index_2"
	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ingest"
	"heaplog_2024/internal/persistence"
	"heaplog_2024/internal/search"
	"heaplog_2024/internal/search/query_language"
)

func TestSearch(t *testing.T) {
	ingestor, _search, fileNames, fileMessages := prepareIndex(t)
	err := ingestor.Run()
	require.NoError(t, err)

	testCases := []struct {
		name     string
		query    string
		dates    [2]*time.Time
		expected []common.FileMessage
	}{
		{
			name:     "no match",
			query:    `unknown`,
			dates:    [2]*time.Time{nil, nil},
			expected: []common.FileMessage(nil),
		},
		{
			name:  "one hit in one file",
			query: `Permission`,
			dates: [2]*time.Time{nil, nil},
			expected: []common.FileMessage{
				fileMessages[fileNames[0]][7],
			},
		},
		{
			name:  "hits in two file (case insensitive)",
			query: `backup`,
			dates: [2]*time.Time{nil, nil},
			expected: []common.FileMessage{
				fileMessages[fileNames[0]][9],
				fileMessages[fileNames[1]][6],
			},
		},
		{
			name:  "regexp case-insensitive",
			query: `~conn`,
			dates: [2]*time.Time{nil, nil},
			expected: []common.FileMessage{
				fileMessages[fileNames[0]][1],
				fileMessages[fileNames[0]][3],
			},
		},
		{
			name:  "regexp case-sensitive",
			query: `@conn`,
			dates: [2]*time.Time{nil, nil},
			expected: []common.FileMessage{
				fileMessages[fileNames[0]][3],
			},
		},
		{
			name:  "case insensitive",
			query: `error config`,
			dates: [2]*time.Time{nil, nil},
			expected: []common.FileMessage{
				fileMessages[fileNames[0]][4],
			},
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name, func(t *testing.T) {
				expr, err := query_language.ParseUserQuery(tc.query)
				require.NoError(t, err)

				messages, err := _search.Search(expr, tc.dates[0], tc.dates[1])
				require.NoError(t, err)

				actualMessages := slices.Collect(messages)
				require.Equal(t, len(tc.expected), len(actualMessages))
				for i, m := range actualMessages {
					require.Equal(t, tc.expected[i], m.FileMessage)
				}
			},
		)
	}

}

func prepareIndex(t *testing.T) (*ingest.Ingestor, *search.Search, []string, map[string][]common.FileMessage) {
	dir := t.TempDir()
	testFile1 := filepath.Join(dir, "test1.log")
	testFile2 := filepath.Join(dir, "test2.log")
	err := common.PopulateFiles(
		map[string][]byte{
			testFile1: []byte(common.SampleLog1),
			testFile2: []byte(common.SampleLog2),
		},
	)
	require.NoError(t, err)

	logger, err := NewLogger("testing")
	require.NoError(t, err)
	duck, err := persistence.NewDuckDB(context.Background(), "", logger)
	require.NoError(t, err)
	require.NoError(t, duck.Migrate())

	ii, err := inverted_index_2.NewInvertedIndex(dir, false)
	require.NoError(t, err)

	tokenize := func(b []byte) [][]byte { return common.Tokenize(b, 4, 8) }
	indexer := ingest.NewIndexer(
		context.Background(),
		logger,
		tokenize,
		func(b []byte) (time.Time, error) {
			return time.Parse(common.TimeFormat, string(b))
		},
	)

	persistentIndex, err := persistence.NewPersistentIndex(duck, ii)
	require.NoError(t, err)

	ingestor := ingest.NewIngestor(
		[]string{testFile1, testFile2},
		regexp.MustCompile(common.MessageStartPattern),
		1_000_000,
		1,
		persistentIndex,
		logger,
		indexer,
	)

	_search := search.NewSearch(context.Background(), tokenize, persistentIndex, logger)

	fileMessages := map[string][]common.FileMessage{
		testFile1: common.MakeFileMessages(testFile1, common.LayoutsSampleLog1),
		testFile2: common.MakeFileMessages(testFile2, common.LayoutsSampleLog2),
	}

	return ingestor, _search, []string{testFile1, testFile2}, fileMessages
}
