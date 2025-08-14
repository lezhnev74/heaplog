package ingest

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
)

func TestIndexer(t *testing.T) {
	// Setup Indexer
	logger := zap.NewNop()
	ix := NewIndexer(
		context.Background(),
		logger,
		func(i []byte) [][]byte {
			return [][]byte{[]byte("test token")}
		},
		func(b []byte) (time.Time, error) {
			return time.Parse(common.TimeFormat, string(b))
		},
	)

	// Prepare test data
	fileName, fileBytes := common.MakeTestFile(t)
	layouts, err := scan(
		fileName,
		len(fileBytes),
		common.MessageStartPattern,
		[]common.Location{{0, len(fileBytes)}},
	)
	require.NoError(t, err)

	// Test indexing
	segments := map[string][][]MessageLayout{
		fileName: {layouts[:1], layouts[1:]},
	}
	var results []taskResult
	for r := range ix.indexSegments(segments) {
		results = append(results, r)
	}

	// Compare results with expected values
	expectedResults := []taskResult{
		{
			task: task{
				file:    fileName,
				layouts: layouts[:1],
			},
			messages: common.SampleLayouts[:1],
			tokens:   [][]byte{[]byte("test token")},
		},
		{
			task: task{
				file:    fileName,
				layouts: layouts[1:],
			},
			messages: common.SampleLayouts[1:],
			tokens:   [][]byte{[]byte("test token")},
		},
	}

	for i, result := range results {
		expected := expectedResults[i]

		// Compare messages
		for j, msg := range result.messages {
			if msg.Location != expected.messages[j].Location {
				t.Errorf(
					"Result %d, message %d: Expected location %v, got %v",
					i,
					j,
					expected.messages[j].Location,
					msg.Location,
				)
			}
			if !msg.Date.Equal(expected.messages[j].Date) {
				t.Errorf("Result %d, message %d: Expected date %v, got %v", i, j, expected.messages[j].Date, msg.Date)
			}
		}

		// Compare tokens
		slices.SortFunc(result.tokens, bytes.Compare)
		slices.SortFunc(expected.tokens, bytes.Compare)
		for j, token := range result.tokens {
			if !bytes.Equal(token, expected.tokens[j]) {
				t.Errorf("Result %d, token %d: Expected %q, got %q", i, j, expected.tokens[j], token)
			}
		}

		// Compare file path
		if result.task.file != expected.task.file {
			t.Errorf("Result %d: Expected file %q, got %q", i, expected.task.file, result.task.file)
		}

		// Compare layouts
		for j, layout := range result.task.layouts {
			if layout != expected.task.layouts[j] {
				t.Errorf("Result %d, layout %d: Expected %v, got %v", i, j, expected.task.layouts[j], layout)
			}
		}
	}

}

func TestBlacklistedFileNotIndexed(t *testing.T) {
	// Setup Indexer with a date parser that will fail
	logger := zap.NewNop()
	ix := NewIndexer(
		context.Background(),
		logger,
		func(i []byte) [][]byte {
			return [][]byte{[]byte("test token")}
		},
		func(b []byte) (time.Time, error) {
			return time.Time{}, fmt.Errorf("unexpected date format")
		},
	)

	// Prepare test data
	fileName, fileBytes := common.MakeTestFile(t)
	layouts, err := scan(
		fileName,
		len(fileBytes),
		common.MessageStartPattern,
		[]common.Location{{0, len(fileBytes)}},
	)
	require.NoError(t, err)

	// First indexing attempt should fail and blacklist the file
	segments := map[string][][]MessageLayout{
		fileName: {layouts},
	}
	results := 0
	for range ix.indexSegments(segments) {
		results++
	}
	require.Equal(t, 0, results, "Expected no results for blacklisted file")

	// Second attempt should be skipped due to blacklisting
	for range ix.indexSegments(segments) {
		t.Error("Expected no results for blacklisted file on second attempt")
	}
}

func TestIndexerContextCancellation(t *testing.T) {
	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	logger := zap.NewNop()

	// Create indexer with slow date parser
	ix := NewIndexer(
		ctx,
		logger,
		func(i []byte) [][]byte {
			return [][]byte{[]byte("test token")}
		},
		func(b []byte) (time.Time, error) {
			time.Sleep(1000 * time.Millisecond) // Simulate slow processing
			return time.Parse(common.TimeFormat, string(b))
		},
	)

	// Prepare test data
	fileName, fileBytes := common.MakeTestFile(t)
	layouts, err := scan(
		fileName,
		len(fileBytes),
		common.MessageStartPattern,
		[]common.Location{{0, len(fileBytes)}},
	)
	require.NoError(t, err)

	segments := map[string][][]MessageLayout{
		fileName: {layouts},
	}

	// Cancel context quickly, before any tasks are started
	go func() { cancel() }()

	// Verify no results are produced
	results := 0
	for range ix.indexSegments(segments) {
		results++
	}
	require.Equal(t, 0, results, "Expected no results after context cancellation")
}
