package heaplog

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog/storage"
	"heaplog/test"
	"os"
	"regexp"
	"testing"
	"time"
)

// Heaplog-level tests are integrational tests, high-level.
// Discovering of files is complicated during high-level tests as globs are not compatible with embedded fs.
var (
	messageStartPattern = regexp.MustCompile(`(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`)
	dateLayout          = "2006-01-02 15:04:05.000000"
)

func TestInvalidQueryId(t *testing.T) {
	hl := initHeaplog(t)
	_, err := hl.QuerySummary("Unknown")
	require.ErrorIs(t, err, storage.ErrNoData)

	_, err = hl.QueryPage("Unknown", 1, 1)
	require.ErrorIs(t, err, storage.ErrNoData)
}

func TestQuery(t *testing.T) {
	type ttest struct {
		query         string
		from, to      *time.Time
		pageSize      int
		expectedTotal int
		// keys here are pages, values are messages
		expectedPages                  [][][]byte
		expectedMinDoc, expectedMaxDoc *time.Time
	}

	tests := []ttest{
		{
			query:         ``, // all
			from:          nil,
			to:            nil,
			pageSize:      100,
			expectedTotal: 8,
			expectedPages: [][][]byte{
				0: {
					[]byte("[2023-01-05 23:40:20.779604] message first\n"),
					[]byte("[2023-01-05 23:45:11.324153] message second\n"),
					[]byte("[2023-01-05 23:46:22.234123] multiline\nmessage\n"),
					[]byte("[2023-01-07 00:00:04.452670] message forth\n"),
					[]byte("[2023-02-01 01:00:00.000001] ip 127.0.0.1\n"),
					[]byte("[2023-02-02 05:00:00.000002] name: John\n"),
					[]byte("[2023-02-03 10:00:00.000003] items\nproduct1\nproduct2\n"),
					[]byte("[2023-02-04 15:00:00.000004] message fifth\n"),
				},
				1: {},
			},
			expectedMinDoc: test.MakeTimeP("2023-01-05 23:40:20.779604"),
			expectedMaxDoc: test.MakeTimeP("2023-02-04 15:00:00.000004"),
		},
		{
			query:         ``,                                           // all
			from:          test.MakeTimeP("2023-01-05 23:47:00.000000"), // respect lower From
			to:            nil,
			pageSize:      3, // paginate
			expectedTotal: 5,
			expectedPages: [][][]byte{
				0: {
					[]byte("[2023-01-07 00:00:04.452670] message forth\n"),
					[]byte("[2023-02-01 01:00:00.000001] ip 127.0.0.1\n"),
					[]byte("[2023-02-02 05:00:00.000002] name: John\n"),
				},
				1: {
					[]byte("[2023-02-03 10:00:00.000003] items\nproduct1\nproduct2\n"),
					[]byte("[2023-02-04 15:00:00.000004] message fifth\n"),
				},
				2: {},
			},
			expectedMinDoc: test.MakeTimeP("2023-01-07 00:00:04.452670"),
			expectedMaxDoc: test.MakeTimeP("2023-02-04 15:00:00.000004"),
		},
		{
			query:         `first`,
			from:          test.MakeTimeP("2023-01-01 00:00:00.000000"),
			to:            nil,
			pageSize:      100,
			expectedTotal: 1,
			expectedPages: [][][]byte{
				0: {[]byte("[2023-01-05 23:40:20.779604] message first\n")},
				1: {},
			},
			expectedMinDoc: test.MakeTimeP("2023-01-05 23:40:20.779604"),
			expectedMaxDoc: test.MakeTimeP("2023-01-05 23:40:20.779604"),
		},
	}

	hl := initHeaplog(t)
	require.NoError(t, hl.ingestor.Ingest())

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, tt.query), func(t *testing.T) {
			// 1. Build a query
			queryId, err := hl.NewQuery(tt.query, tt.pageSize, tt.from, tt.to)
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			// 2. Request stats
			summary, err := hl.QuerySummary(queryId)
			require.NoError(t, err)

			if tt.from != nil {
				require.True(t, tt.from.Equal(*summary.From))
			} else {
				require.Nil(t, summary.From)
			}

			if tt.to != nil {
				require.Nil(t, summary.To)
			} else {
				require.Nil(t, summary.To)
			}

			require.Equal(t, tt.query, summary.Text)
			require.Equal(t, tt.expectedTotal, summary.Total)

			require.True(t, tt.expectedMinDoc.Equal(*summary.MinDoc))
			require.True(t, tt.expectedMaxDoc.Equal(*summary.MaxDoc))

			// 3. Read pages
			for page, expectedMessages := range tt.expectedPages {
				messages, err := hl.QueryPage(queryId, page, tt.pageSize)
				require.NoError(t, err)
				require.Equal(t, expectedMessages, messages)
			}
		})
	}
}

func TestDescreteCalculation(t *testing.T) {
	type tt struct {
		from, to      time.Time
		descreteCount int
		expectedUnit  string
	}
	tests := []tt{
		{test.MakeTimeV("2020-01-01 00:00:00.000000"), test.MakeTimeV("2023-03-15 00:00:00.000000"), 1, "year"},
		{test.MakeTimeV("2020-01-01 00:00:00.000000"), test.MakeTimeV("2023-03-15 00:00:00.000000"), 50, "month"},
		{test.MakeTimeV("2020-01-01 00:00:00.000000"), test.MakeTimeV("2020-01-15 00:00:00.000000"), 20, "day"},
		{test.MakeTimeV("2020-01-01 00:00:00.000000"), test.MakeTimeV("2020-01-03 23:59:59.000000"), 80, "hour"},
		{test.MakeTimeV("2020-01-01 00:00:00.000000"), test.MakeTimeV("2020-01-01 23:59:59.000000"), 2000, "minute"},
		{test.MakeTimeV("2023-01-05 23:40:00.000000"), test.MakeTimeV("2023-01-05 23:44:59.000000"), 5, "minute"},
		{test.MakeTimeV("2023-01-05 23:40:00.000000"), test.MakeTimeV("2023-01-05 23:40:59.000000"), 60, "second"},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			result := calculateDiscreteUnit(tt.from, tt.to, tt.descreteCount)
			require.Equal(t, tt.expectedUnit, result)
		})
	}
}

func initHeaplog(t *testing.T) *Heaplog {
	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	globs := []string{""} // todo how to use globs in tests?
	boundTokenizerFunc, unboundTokenizerFunc := test.PrepareTokenizers()
	hl, err := NewHeaplog(
		storageRoot,
		messageStartPattern,
		dateLayout,
		globs,
		time.Millisecond,
		boundTokenizerFunc,
		unboundTokenizerFunc,
		80, // small segments will contain a single message
	)
	require.NoError(t, err)

	// manually discover files: hl.DiscoverFiles()
	logFiles := test.PrepareDataSourceFiles(t)
	_, _, err = hl.storage.CheckInFiles(logFiles)
	require.NoError(t, err)

	return hl
}
