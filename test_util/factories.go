package test_util

import (
	"context"
	"database/sql"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2"
	"github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"
	"heaplog_2024/db"
	"heaplog_2024/ingest"
	"heaplog_2024/scanner"
	"heaplog_2024/tokenizer"
	"os"
	"testing"
	"time"
)

var (
	MessageStartPattern = `(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`
	TimeFormat          = "2006-01-02T15:04:05.000000-07:00"
)

func PrepareTempDir(t *testing.T) string {
	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	return storageRoot
}

func PrepareTestDb(t *testing.T) (*db.DbContainer, string) {
	storageRoot := PrepareTempDir(t)

	connector, err := db.PrepareDuckDB(storageRoot, 100)
	require.NoError(t, err)
	_db := sql.OpenDB(connector)
	err = db.Migrate(_db)
	require.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	messageAppender, err := duckdb.NewAppenderFromConn(conn, "", "file_segments_messages")
	require.NoError(t, err)
	resultsAppender, err := duckdb.NewAppenderFromConn(conn, "", "query_results")
	require.NoError(t, err)

	dbContainer := &db.DbContainer{
		DB:         _db,
		FilesDb:    db.NewFilesDb(_db),
		SegmentsDb: db.NewSegmentsDb(_db),
		MessagesDb: db.NewMessagesDb(_db, messageAppender),
		QueryDB:    db.NewQueryDb(_db, resultsAppender),
	}

	return dbContainer, storageRoot
}

func PrepareTestIngest(t *testing.T, segmentSize uint64, storageRoot string, db *db.DbContainer) (*ingest.Ingest, *inverted_index_2.InvertedIndex) {

	tok := func(in []byte) [][]byte {
		return tokenizer.Tokenize(in, 4, 10)
	}

	ii, err := inverted_index_2.NewInvertedIndex(storageRoot)
	require.NoError(t, err)

	s := func(file string) ([]scanner.MessageLayout, error) {
		it, err := scanner.UgScan(file, MessageStartPattern)
		require.NoError(t, err)
		return go_iterators.ToSlice(it), nil
	}
	pd := func(b []byte) (time.Time, error) {
		return time.Parse(TimeFormat, string(b))
	}

	ing := ingest.NewIngest(s, pd, tok, db, ii, segmentSize, 1)

	return ing, ii
}