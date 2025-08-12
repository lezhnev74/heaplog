package ingest

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ingest/scanner"
	"heaplog_2024/internal/ingest/tokenizer"
)

const MessageStartPattern = `(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`
const TimeFormat = "2006-01-02T15:04:05.000000-07:00"
const fileContents = `
[2024-07-30T00:00:04.769958+00:00] message first
[2024-07-30T00:00:12.285087+00:00] message second
[2024-07-30T00:00:12.967490+00:00] message third
`

func TestIndexer(t *testing.T) {

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.log")

	err := common.PopulateFiles(
		map[string][]byte{
			testFile: []byte(fileContents),
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	// Setup indexer
	logger := zap.NewNop()
	bufPool := common.NewBufferPool([]int{1024})

	ix := &indexer{
		workers:  1, // predictable results
		bufPool:  bufPool,
		logger:   logger,
		tokenize: func(i []byte) [][]byte { return tokenizer.Tokenize(i, 4, 8) },
		parseDate: func(b []byte) (time.Time, error) {
			return time.Parse(TimeFormat, string(b))
		},
	}

	// Prepare test data
	fileBytes := []byte(fileContents)
	layouts, err := scanner.Scan(
		testFile,
		len(fileBytes),
		MessageStartPattern,
		[]common.Location{common.Location{0, len(fileBytes)}},
	)
	require.NoError(t, err)

	segments := map[string][][]scanner.MessageLayout{
		testFile: {layouts[:1], layouts[1:]},
	}

	// Test indexing
	var results []taskResult
	for r := range ix.indexSegments(segments) {
		results = append(results, r)
	}

	expectedResults := []taskResult{
		{
			task: task{
				file:    testFile,
				layouts: layouts[:1],
			},
			messages: []Message{
				{
					Location: common.Location{From: 1, To: 50},
					Date:     time.Date(2024, 7, 30, 0, 0, 4, 769958000, time.UTC),
				},
			},
			tokens: [][]byte{[]byte("message"), []byte("first")},
		},
		{
			task: task{
				file:    testFile,
				layouts: layouts[1:],
			},
			messages: []Message{
				{
					Location: common.Location{From: 50, To: 100},
					Date:     time.Date(2024, 7, 30, 0, 0, 12, 285087000, time.UTC),
				},
				{
					Location: common.Location{From: 100, To: 149},
					Date:     time.Date(2024, 7, 30, 0, 0, 12, 967490000, time.UTC),
				},
			},
			tokens: [][]byte{[]byte("message"), []byte("second"), []byte("third")},
		},
	}

	// Compare results with expected values
	if len(results) != len(expectedResults) {
		t.Fatalf("Expected %d results, got %d", len(expectedResults), len(results))
	}

	for i, result := range results {
		expected := expectedResults[i]

		// Compare messages
		if len(result.messages) != len(expected.messages) {
			t.Errorf("Result %d: Expected %d messages, got %d", i, len(expected.messages), len(result.messages))
			continue
		}
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
		if len(result.tokens) != len(expected.tokens) {
			t.Errorf("Result %d: Expected %d tokens, got %d", i, len(expected.tokens), len(result.tokens))
			continue
		}
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
		if len(result.task.layouts) != len(expected.task.layouts) {
			t.Errorf("Result %d: Expected %d layouts, got %d", i, len(expected.task.layouts), len(result.task.layouts))
			continue
		}
		for j, layout := range result.task.layouts {
			if layout != expected.task.layouts[j] {
				t.Errorf("Result %d, layout %d: Expected %v, got %v", i, j, expected.task.layouts[j], layout)
			}
		}
	}

}
