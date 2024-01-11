package search

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"heaplog/common"
	"heaplog/ingest"
	"heaplog/test"
	"slices"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	selector, files := ingestFiles(
		t,
		10, // this ingests every message in a separate segment
	)
	filenames := maps.Keys(files)

	// In this test every segment contains a single message.
	// So it is easy to test segment selection.
	// Message locations:
	// file1.log: {0,43}, {43,87}, {87,134}, {134,176}
	// file2.log: {0,42}, {42,82}, {82,135}, {135,177}

	fullScan := map[common.DataSourceHash][]common.IndexedMessage{
		common.HashFile(filenames[0]): {
			{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), false},
			{-1, common.Location{43, 87}, test.MakeTimeV("2023-01-05 23:45:11.324153"), false},
			{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), false},
			{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), false},
		},
		common.HashFile(filenames[1]): {
			{-1, common.Location{0, 42}, test.MakeTimeV("2023-02-01 01:00:00.000001"), false},
			{-1, common.Location{42, 82}, test.MakeTimeV("2023-02-02 05:00:00.000002"), false},
			{-1, common.Location{82, 135}, test.MakeTimeV("2023-02-03 10:00:00.000003"), false},
			{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), false},
		},
	}

	time.Sleep(time.Millisecond * 1000)

	slices.Sort(filenames)

	type _test struct {
		query            string
		minDate, maxDate *time.Time
		expectedSegments map[common.DataSourceHash][]common.IndexedMessage
	}

	tests := []_test{
		{
			"", // full scan
			nil, nil,
			fullScan,
		},
		{
			"", // full scan + dates
			test.MakeTimeP("2023-01-05 23:45:11.324153"),
			test.MakeTimeP("2023-02-02 05:00:00.000002"),
			map[common.DataSourceHash][]common.IndexedMessage{
				common.HashFile(filenames[0]): {
					{-1, common.Location{43, 87}, test.MakeTimeV("2023-01-05 23:45:11.324153"), false},
					{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), false},
					{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), false},
				},
				common.HashFile(filenames[1]): {
					{-1, common.Location{0, 42}, test.MakeTimeV("2023-02-01 01:00:00.000001"), false},
					{-1, common.Location{42, 82}, test.MakeTimeV("2023-02-02 05:00:00.000002"), false},
				},
			},
		},
		{
			"~.*", // RE -> fullScan
			nil, nil,
			fullScan,
		},
		{
			"~.*",
			test.MakeTimeP("2023-01-07 00:00:04.452670"),
			nil,
			map[common.DataSourceHash][]common.IndexedMessage{
				common.HashFile(filenames[0]): {
					{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), false},
				},
				common.HashFile(filenames[1]): {
					{-1, common.Location{0, 42}, test.MakeTimeV("2023-02-01 01:00:00.000001"), false},
					{-1, common.Location{42, 82}, test.MakeTimeV("2023-02-02 05:00:00.000002"), false},
					{-1, common.Location{82, 135}, test.MakeTimeV("2023-02-03 10:00:00.000003"), false},
					{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), false},
				},
			},
		},
		{
			"first", // long token: use II
			nil, nil,
			map[common.DataSourceHash][]common.IndexedMessage{
				common.HashFile(filenames[0]): {
					{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), false},
				},
			},
		},
		{
			"me", // short token: full scan
			nil, nil,
			fullScan,
		},
		{
			"first.tw", // long token "first": II AND short token "tw": full scan -> use II
			nil, nil,
			map[common.DataSourceHash][]common.IndexedMessage{
				common.HashFile(filenames[0]): {
					{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), false},
				},
			},
		},
		{
			"fi.r.st", // short tokens -> fullScan
			nil, nil,
			fullScan,
		},
		{
			"first OR tw", // long token "first": II OR short token "tw": full scan -> fullScan
			nil, nil,
			fullScan,
		},
		{
			"mess and !first", // mess: II, negation: fullScan
			nil, nil,
			map[common.DataSourceHash][]common.IndexedMessage{
				common.HashFile(filenames[0]): {
					{-1, common.Location{0, 43}, test.MakeTimeV("2023-01-05 23:40:20.779604"), false},
					{-1, common.Location{43, 87}, test.MakeTimeV("2023-01-05 23:45:11.324153"), false},
					{-1, common.Location{87, 134}, test.MakeTimeV("2023-01-05 23:46:22.234123"), false},
					{-1, common.Location{134, 177}, test.MakeTimeV("2023-01-07 00:00:04.452670"), false},
				},
				common.HashFile(filenames[1]): {
					{-1, common.Location{135, 178}, test.MakeTimeV("2023-02-04 15:00:00.000004"), false},
				},
			},
		},
		{
			"mul or ms", // both use fullScan: fullScan
			nil, nil,
			fullScan,
		},
		{
			"(mes and sec) OR pr", // all full scan
			nil, nil,
			fullScan,
		},
		{
			"!mes", // full scan
			nil, nil,
			fullScan,
		},
		{
			"!mess", // negation -> fullscan
			nil, nil,
			fullScan,
		},
		{
			"unknown", // big term -> II
			nil, nil,
			map[common.DataSourceHash][]common.IndexedMessage{},
		},
		{
			".*", // short term -> fullScan
			nil, nil,
			fullScan,
		},
		{
			".* AND unknown", // full scan + nothing -> nothing
			nil, nil,
			map[common.DataSourceHash][]common.IndexedMessage{},
		},
		{
			".* OR unknown", // full scan OR nothing -> full scan
			nil, nil,
			fullScan,
		},
		{
			"unknown.glued.together", // long token -> II
			nil, nil,
			map[common.DataSourceHash][]common.IndexedMessage{},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, tt.query), func(t *testing.T) {
			expr, err := ParseUserQuery(tt.query)
			require.NoError(t, err)

			actualSegments, err := selector.SelectSegments(expr, tt.minDate, tt.maxDate)
			require.NoError(t, err)

			segments, err := selector.storage.ReadSegmentLocationsPerDS(actualSegments)
			require.NoError(t, err)

			require.EqualValues(t, tt.expectedSegments, segments)
		})
	}
}

func ingestFiles(t *testing.T, indexableSegmentSize int64) (*SegmentsSelector, map[string]int64) {
	// Prepare the source files and index them:
	storage, _indexer, _ := test.PrepareServices(t, indexableSegmentSize)

	files := test.PrepareDataSourceFiles(t)
	_, _, err := storage.CheckInFiles(files)
	require.NoError(t, err)

	ingestor := ingest.NewIngestor(
		storage,
		_indexer,
		indexableSegmentSize,
		1,
	)
	require.NoError(t, ingestor.Ingest())

	tokenizer, unboundTokenizerFunc := test.PrepareTokenizers()
	selector := NewSegmentSelector(storage, unboundTokenizerFunc, tokenizer)

	return selector, files
}
