package storage_test

import (
	"github.com/stretchr/testify/require"
	"heaplog/common"
	"heaplog/storage"
	"heaplog/test"
	"log"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

var noop = func(segmentId int) error { return nil }

func TestOpenStorage(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)

	s, err := storage.NewStorage(dirPath, time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestSegmentMerging(t *testing.T) {
	// Plan:
	// 1. Put a few small segment and a few messages belonging to them
	// 2. Call segment merging
	// 3. Assert that small segments are merged to a big ones (with respect to the segment size)

	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	s.CheckInFiles(map[string]int64{
		"file1": 30,
	})
	ds := common.HashFile("file1")

	// Exec:
	// 1. Put a few small segment and a few messages belonging to them
	t1 := time.UnixMicro(100_000_000)
	t2 := time.UnixMicro(200_000_000)
	t3 := time.UnixMicro(300_000_000)
	segId1, err := s.CheckInSegment(common.IndexedSegment{ds, []common.IndexedMessage{{1, common.Location{0, 10}, t1, false}}}, []string{"term1"})
	require.NoError(t, err)
	segId2, err := s.CheckInSegment(common.IndexedSegment{ds, []common.IndexedMessage{{1, common.Location{10, 20}, t2, false}}}, []string{"term1"})
	require.NoError(t, err)
	segId3, err := s.CheckInSegment(common.IndexedSegment{ds, []common.IndexedMessage{{1, common.Location{20, 30}, t3, false}}}, []string{"term1"})
	require.NoError(t, err)

	// 2. Call segment merging
	ok, err := s.MergeSegments(15) // merged segments take up to 150% of segment size
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.MergeSegments(15)
	require.NoError(t, err)
	require.False(t, ok)

	// 3. Assert that small segments are merged to a big ones (with respect to the segment size)
	actualSegments, err := s.GetSegments([]int{segId1, segId2, segId3})
	require.NoError(t, err)
	require.Len(t, actualSegments, 2) // one segment was merged

	expectedSegments := []common.IndexedSegment{
		{ds, []common.IndexedMessage{
			{-1, common.Location{0, 10}, t1.UTC(), false},
			{-1, common.Location{10, 20}, t2.UTC(), false},
		}},
		{ds, []common.IndexedMessage{{-1, common.Location{20, 30}, t3.UTC(), false}}},
	}
	test.RemoveMessageIds(actualSegments)
	require.Equal(t, expectedSegments, actualSegments)
}

func TestQuerySummary(t *testing.T) {
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	s.CheckInFiles(map[string]int64{
		"file1": 120,
	})
	ds := common.HashFile("file1")

	// 1. Ingest data
	s1 := common.IndexedSegment{
		DataSource: ds,
		Messages: []common.IndexedMessage{
			{Loc: common.Location{0, 100}, Date: test.MakeTimeV("2023-01-05 00:00:00.000000")},
			{Loc: common.Location{100, 120}, Date: test.MakeTimeV("2023-01-06 00:00:00.000000")},
		},
	}
	s1Id, err := s.CheckInSegment(s1, []string{})
	require.NoError(t, err)

	time.Sleep(30 * time.Millisecond)

	// 2. Build a query and report matched messages
	queryText := "find me"
	queryHash := "abc"
	from := test.MakeTimeP("2023-01-01 12:00:00.000000")
	require.NoError(t, s.CheckInQuery(queryHash, queryText, from, nil))

	// test.DumpTable(s.GetDb(), "file_segments", 6)
	// test.DumpTable(s.GetDb(), "file_segments_messages", 5)
	potentialSegments, err := s.GetSegments([]int{s1Id}) // read back checked in segments
	require.NoError(t, err)

	matchedMessage := common.MatchedMessage{
		Id:        potentialSegments[0].Messages[0].Id,
		QueryHash: queryHash,
	}
	s.CheckInQueryMessage(matchedMessage)

	// 3. Build Summary of the Query
	time.Sleep(time.Millisecond * 50)

	summary, err := s.GetQuerySummary(queryHash)
	require.NoError(t, err)

	expectedSummary := common.QuerySummary{
		QueryId:  queryHash,
		Complete: false,
		Text:     queryText,
		From:     from,
		To:       nil,
		Total:    1,
		MinDoc:   test.MakeTimeP("2023-01-05 00:00:00.000000"),
		MaxDoc:   test.MakeTimeP("2023-01-05 00:00:00.000000"),
	}
	require.EqualValues(t, expectedSummary, summary)

	err = s.CheckInFinishedQuery(queryHash)
	require.NoError(t, err)

	summary, err = s.GetQuerySummary(queryHash)
	require.NoError(t, err)
	require.True(t, summary.Complete)
}

func TestItRemovesObsoleteSegments(t *testing.T) {
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	files := map[string]int64{
		"file1": 100,
		"file2": 200,
	}

	_, _, err := s.CheckInFiles(files)
	require.NoError(t, err)

	s1 := common.IndexedSegment{
		common.HashFile("file1"),
		[]common.IndexedMessage{
			{Loc: common.Location{0, 10}, Date: time.Now()},
		},
	}
	s.CheckInSegment(s1, []string{})

	delete(files, "file1")

	obsolete, _, err := s.CheckInFiles(files)
	require.NoError(t, err)
	require.Contains(t, obsolete, "file1")

	segments, err := s.ReadIndexedLocations(common.HashFile("file1"))
	require.NoError(t, err)
	require.Empty(t, segments)
}

func TestFilesCheckingIn(t *testing.T) {
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	// 1. First check in
	files := map[string]int64{
		"file1": 100,
		"file2": 200,
	}
	obsolete, newFiles, err := s.CheckInFiles(files)
	require.NoError(t, err)

	slices.Sort(newFiles)
	require.Equal(t, []string{"file1", "file2"}, newFiles)

	actualFiles, err := s.AllFiles()
	require.NoError(t, err)
	require.EqualValues(t, files, actualFiles)

	// 2. Second check in
	files = map[string]int64{
		"file1":   120,
		"fileNEW": 500,
		"file4":   35,
	}

	obsolete, newFiles, err = s.CheckInFiles(files)
	require.NoError(t, err)
	require.NoError(t, err)

	slices.Sort(newFiles)
	require.Equal(t, []string{"file4", "fileNEW"}, newFiles)
	require.Equal(t, []string{"file2"}, obsolete)

	actualFiles, err = s.AllFiles()
	require.NoError(t, err)
	require.EqualValues(t, files, actualFiles)
}

func TestFilesCheckingInConcurrency(t *testing.T) {
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	// 1. Make N concurrent check-ins
	files := map[string]int64{
		"file1": 100,
		"file2": 200,
		"file3": 150,
		"file4": 10,
		"file5": 0,
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.CheckInFiles(files)
		}()
	}
	wg.Wait()

	// 2. See no duplication exists
	actualFiles, err := s.AllFiles()
	require.NoError(t, err)
	require.EqualValues(t, files, actualFiles)

	// 3. Now change the list of files and see files are removed
	files = map[string]int64{
		"file1":   120,
		"fileNEW": 500,
		"file4":   35,
	}

	obsolete, _, err := s.CheckInFiles(files)
	require.NoError(t, err)

	slices.Sort(obsolete)
	require.EqualValues(t, []string{"file2", "file3", "file5"}, obsolete)

	actualFiles, err = s.AllFiles()
	require.NoError(t, err)

	require.EqualValues(t, files, actualFiles)

	// read file path by hash
	f, err := s.ReadFileByHash(common.HashFile("file1"))
	require.NoError(t, err)
	require.Equal(t, "file1", f)
}

func TestQueryAggregation(t *testing.T) {
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	// Populate query messages
	s.CheckInFiles(map[string]int64{
		"file1": 200,
	})
	ds := common.HashFile("file1")

	s1 := common.IndexedSegment{
		DataSource: ds,
		Messages: []common.IndexedMessage{
			{Loc: common.Location{10, 11}, Date: test.MakeTimeV("2020-01-01 00:00:00.001000")},
			{Loc: common.Location{11, 12}, Date: test.MakeTimeV("2020-01-01 00:00:00.001000")},
			{Loc: common.Location{12, 13}, Date: test.MakeTimeV("2020-01-01 00:00:00.002000")},
			{Loc: common.Location{13, 14}, Date: test.MakeTimeV("2020-01-01 00:00:01.004000")},
			{Loc: common.Location{14, 15}, Date: test.MakeTimeV("2020-01-01 00:00:01.005000")},
			{Loc: common.Location{15, 16}, Date: test.MakeTimeV("2020-01-01 00:00:02.000000")},
			{Loc: common.Location{16, 17}, Date: test.MakeTimeV("2020-01-01 00:01:00.000000")},
			{Loc: common.Location{17, 18}, Date: test.MakeTimeV("2020-01-01 00:01:01.000000")},
			{Loc: common.Location{18, 19}, Date: test.MakeTimeV("2020-01-01 01:00:00.000000")},
			{Loc: common.Location{19, 20}, Date: test.MakeTimeV("2020-01-01 01:01:00.000000")},
			{Loc: common.Location{20, 21}, Date: test.MakeTimeV("2020-01-01 01:01:01.000000")},
			{Loc: common.Location{21, 22}, Date: test.MakeTimeV("2020-01-02 00:00:00.000000")},
			{Loc: common.Location{22, 23}, Date: test.MakeTimeV("2020-02-01 00:00:00.000000")},
			{Loc: common.Location{23, 24}, Date: test.MakeTimeV("2021-01-01 00:00:00.000000")},
		},
	}
	segmentId, err := s.CheckInSegment(s1, []string{})
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond) // wait for the flush

	// Make a query that matches all messages
	queryText := "~.*"
	queryHash := "abc"
	require.NoError(t, s.CheckInQuery(queryHash, queryText, nil, nil))

	segments, _ := s.GetSegments([]int{segmentId})
	for _, m := range segments[0].Messages {
		s.CheckInQueryMessage(common.MatchedMessage{Id: m.Id, QueryHash: queryHash})
	}

	// Now make expectations on aggregated values
	type expect struct {
		testName, unit string
		from, to       time.Time
		expectedResult map[int64]int64
		expectedError  string
	}

	expectations := []expect{
		{
			testName: "unbound seconds",
			unit:     "second",
			from:     test.MakeTimeV("2020-01-01 00:00:00.000000"),
			to:       test.MakeTimeV("2023-01-01 00:00:00.000000"),
			expectedResult: map[int64]int64{
				test.MakeTimeV("2020-01-01 00:00:00.000000").UnixMilli(): 3,
				test.MakeTimeV("2020-01-01 00:00:01.000000").UnixMilli(): 2,
				test.MakeTimeV("2020-01-01 00:00:02.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-01 00:01:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-01 00:01:01.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-01 01:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-01 01:01:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-01 01:01:01.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-02 00:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-02-01 00:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2021-01-01 00:00:00.000000").UnixMilli(): 1,
			},
			expectedError: "",
		},
		{
			testName: "unbound minutes",
			unit:     "minute",
			from:     test.MakeTimeV("2020-01-01 00:00:00.000000"),
			to:       test.MakeTimeV("2023-01-01 00:00:00.000000"),
			expectedResult: map[int64]int64{
				test.MakeTimeV("2020-01-01 00:00:00.000000").UnixMilli(): 6,
				test.MakeTimeV("2020-01-01 00:01:00.000000").UnixMilli(): 2,
				test.MakeTimeV("2020-01-01 01:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-01-01 01:01:00.000000").UnixMilli(): 2,
				test.MakeTimeV("2020-01-02 00:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-02-01 00:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2021-01-01 00:00:00.000000").UnixMilli(): 1,
			},
			expectedError: "",
		},
		{
			testName: "unbound hours",
			unit:     "hour",
			from:     test.MakeTimeV("2020-01-01 00:00:00.000000"),
			to:       test.MakeTimeV("2023-01-01 00:00:00.000000"),
			expectedResult: map[int64]int64{
				test.MakeTimeV("2020-01-01 00:00:00.000000").UnixMilli(): 8,
				test.MakeTimeV("2020-01-01 01:00:00.000000").UnixMilli(): 3,
				test.MakeTimeV("2020-01-02 00:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2020-02-01 00:00:00.000000").UnixMilli(): 1,
				test.MakeTimeV("2021-01-01 00:00:00.000000").UnixMilli(): 1,
			},
			expectedError: "",
		},
		{
			testName: "bound hours",
			unit:     "hour",
			from:     test.MakeTimeV("2020-01-01 00:00:00.000000"),
			to:       test.MakeTimeV("2020-01-02 00:00:00.000000"),
			expectedResult: map[int64]int64{
				test.MakeTimeV("2020-01-01 00:00:00.000000").UnixMilli(): 8,
				test.MakeTimeV("2020-01-01 01:00:00.000000").UnixMilli(): 3,
			},
			expectedError: "",
		},
		{
			testName:       "invalid unit",
			unit:           "unknown",
			from:           test.MakeTimeV("2020-01-01 00:00:00.000000"),
			to:             test.MakeTimeV("2020-01-02 00:00:00.000000"),
			expectedResult: map[int64]int64{},
			expectedError:  "invalid unit provided: unknown",
		},
	}

	for _, e := range expectations {
		t.Run(e.testName, func(t *testing.T) {
			log.Printf("From: %d, To: %d", e.from.UnixMicro(), e.to.UnixMicro())
			result, err := s.QueryAggregate(queryHash, e.unit, e.from, e.to)
			if err != nil {
				require.ErrorContains(t, err, e.expectedError)
			} else {
				require.EqualValues(t, e.expectedResult, result)
			}
		})
	}

}

func prepareStorage(t *testing.T) (path string, s *storage.Storage) {
	path, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	s, err = storage.NewStorage(path, time.Millisecond)
	require.NoError(t, err)

	return
}