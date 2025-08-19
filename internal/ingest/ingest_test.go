package ingest

import (
	"context"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/persistence"
)

type MockFileIndex struct {
	*persistence.DuckDB
}

func (m *MockFileIndex) PutSegment(file string, terms [][]byte, messages []common.Message) (int, error) {
	return m.DuckDB.PutSegment(file, messages)
}

func TestIngesting(t *testing.T) {
	fileName, _ := common.MakeTestFile(t)
	ingestor, duck := makeTestIngestor(t, []string{fileName})
	require.NoError(t, ingestor.Run())

	// Analyze the state
	messagesSeq, err := duck.GetMessages(nil, nil, nil)
	require.NoError(t, err)
	messages := slices.Collect(messagesSeq)
	require.Equal(t, len(common.SampleLayouts), len(messages))

	for _, l := range common.SampleLayouts {
		found := false
		for _, m := range messages {
			if l == m.Message {
				found = true
				break
			}
		}
		require.True(t, found)
	}
}

func TestMisalignedSegments(t *testing.T) {
	fileName, _ := common.MakeTestFile(t)
	ingestor, duck := makeTestIngestor(t, []string{fileName})
	ingestor.segmentLen = 1_000_000

	// Put misaligned segment
	_, err := duck.PutSegment(
		fileName, []common.Message{
			{MessageLayout: common.MessageLayout{Loc: common.Location{From: 0, To: 55}}, Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00")},
		},
	)
	require.NoError(t, err)

	// Run
	require.NoError(t, ingestor.Run())

	// Analyze the state
	fileSegments, err := duck.GetSegments()
	require.NoError(t, err)
	expected := map[string][]common.Location{
		fileName: {
			{From: 1, To: len(common.SampleLog)},
		},
	}
	require.Equal(t, expected, fileSegments)
}

func TestTrailingSegmentIndexing(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.log")
	err := common.PopulateFiles(
		map[string][]byte{
			testFile: []byte(common.SampleLog),
		},
	)
	require.NoError(t, err)

	ingestor, duck := makeTestIngestor(t, []string{testFile})
	ingestor.segmentLen = 1_000_000 // make it big so the trailing segment is half-full

	// Run
	require.NoError(t, ingestor.Run())

	// Analyze the state
	fileSegments, err := duck.GetSegments()
	require.NoError(t, err)
	expected := map[string][]common.Location{
		testFile: {
			{From: 1, To: len(common.SampleLog)},
		},
	}
	require.Equal(t, expected, fileSegments)

	// Now Add to the file
	newLog := common.SampleLog + common.SampleLog // double it
	err = common.PopulateFiles(
		map[string][]byte{
			testFile: []byte(newLog),
		},
	)
	require.NoError(t, err)

	// Run again
	require.NoError(t, ingestor.Run())

	// Analyze the state
	fileSegments, err = duck.GetSegments()
	require.NoError(t, err)
	expected = map[string][]common.Location{
		testFile: {
			{From: 1, To: len(common.SampleLog) * 2},
		},
	}
	require.Equal(t, expected, fileSegments)
}

func TestReconcileMissing(t *testing.T) {
	fileName, _ := common.MakeTestFile(t)
	ingestor, duck := makeTestIngestor(t, []string{fileName})

	// Put outdated segment
	_, err := duck.PutSegment(
		"unknown", []common.Message{
			{MessageLayout: common.MessageLayout{Loc: common.Location{From: 0, To: 10}}, Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00")},
		},
	)
	require.NoError(t, err)
	messagesSeq1, err := duck.GetMessages(nil, nil, nil)
	require.NoError(t, err)
	messages1 := slices.Collect(messagesSeq1)
	require.Equal(
		t,
		[]common.FileMessage{
			common.FileMessage{
				File: "unknown",
				Message: common.Message{
					MessageLayout: common.MessageLayout{Loc: common.Location{From: 0, To: 10}}, Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
				},
			},
		},
		messages1,
	)

	// Run and reconcile
	require.NoError(t, ingestor.Run())

	// Analyze the state
	messagesSeq, err := duck.GetMessages(nil, nil, nil)
	require.NoError(t, err)
	messages := slices.Collect(messagesSeq)
	require.Equal(t, len(common.SampleLayouts), len(messages))

	for _, l := range common.SampleLayouts {
		found := false
		for _, m := range messages {
			if l == m.Message {
				found = true
				break
			}
		}
		require.True(t, found)
	}
}

func makeTestIngestor(t *testing.T, globs []string) (*Ingestor, *persistence.DuckDB) {
	logger := zap.NewNop()
	indexer := NewIndexer(
		context.Background(),
		logger,
		func(i []byte) [][]byte {
			return [][]byte{[]byte("test token")}
		},
		func(b []byte) (time.Time, error) {
			return time.Parse(common.TimeFormat, string(b))
		},
	)
	duck, err := persistence.NewDuckDB(context.Background(), "")
	require.NoError(t, err)
	require.NoError(t, duck.Migrate())
	ingestor := NewIngestor(
		globs,
		regexp.MustCompile(common.MessageStartPattern),
		1,
		1,
		&MockFileIndex{duck},
		logger,
		indexer,
	)
	return ingestor, duck
}
