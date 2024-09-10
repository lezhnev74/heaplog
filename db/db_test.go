package db_test

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2"
	"github.com/stretchr/testify/require"
	"heaplog_2024/db"
	"heaplog_2024/test_util"
	"os"
	"testing"
)

func TestClearUp(t *testing.T) {
	// Populate database with a few files and queries
	_db, storageRoot := test_util.PrepareTestDb(t)
	defer os.RemoveAll(storageRoot)

	ii, err := inverted_index_2.NewInvertedIndex(storageRoot)
	require.NoError(t, err)

	_, err = _db.Exec(`INSERT INTO files VALUES (1, 'path1'), (2, 'path2')`)
	require.NoError(t, err)

	_, err = _db.Exec(`INSERT INTO file_segments VALUES (1, 1, 0,10,100,200), (2, 2, 0,10,100,200)`)
	require.NoError(t, err)

	_, err = _db.Exec(`INSERT INTO file_segments_messages VALUES (1,0,1,2), (2,0,1,2)`)
	require.NoError(t, err)

	_, err = _db.Exec(`INSERT INTO query_results VALUES (1, 1, 0, 1, 100), (1, 2, 0, 1, 100)`)
	require.NoError(t, err)

	// ClearUp
	_, err = _db.Exec(`DELETE FROM files WHERE id=1`)
	require.NoError(t, err)

	require.NoError(t, db.ClearUp(_db, ii))

	// Assert files
	files, err := _db.AllFiles()
	require.NoError(t, err)
	require.Equal(t, []string{"path2"}, files)

	// Assert segments
	segmentIds, err := _db.SegmentsDb.AllSegmentIds(nil, nil)
	require.NoError(t, err)
	require.Equal(t, []uint32{2}, segmentIds)

	// Assert segment messages
	messagesIt, err := _db.MessagesDb.AllMessagesIt()
	require.NoError(t, err)
	messages := go_iterators.ToSlice(messagesIt)
	require.Len(t, messages, 1)

	// Assert query results
	messages, err = _db.QueryDB.Page(1, nil, nil, 0, 100)
	require.NoError(t, err)
	require.Len(t, messages, 1)
}
