package ingest

import (
	"context"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/duckdb"
)

func TestIngesting(t *testing.T) {
	fileName, _ := common.MakeTestFile(t)
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
	duck, err := duckdb.NewDuckDB(context.Background(), "")
	require.NoError(t, err)
	require.NoError(t, duck.Migrate())
	ingestor := NewIngestor(
		[]string{fileName},
		regexp.MustCompile(common.MessageStartPattern),
		1,
		1,
		duck,
		logger,
		indexer,
	)
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
