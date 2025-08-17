package ingest

import (
	"context"
	"regexp"
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
	indexDb, err := duckdb.NewDuckDB(context.Background(), "")
	require.NoError(t, err)
	require.NoError(t, indexDb.Migrate())
	ingestor := NewIngestor(
		[]string{fileName},
		regexp.MustCompile(common.MessageStartPattern),
		1,
		1,
		indexDb,
		logger,
		indexer,
	)
	require.NoError(t, ingestor.Run())
}
