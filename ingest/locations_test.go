package ingest

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog_2024/common"
	"heaplog_2024/db"
	"os"
	"path"
	"testing"
	"time"
)

func TestSegmentSelection(t *testing.T) {
	_db, sdb, fdb, root := PrepareSegmentsForTesting(t)
	defer os.RemoveAll(root)
	dbc := &db.DbContainer{fdb, nil, sdb, nil, _db}

	file1 := path.Join(root, "file1.log")
	_, _, err := fdb.CheckInFiles([]string{file1})
	require.NoError(t, err)
	file1Id, _ := fdb.GetFileId(file1)

	PopulateFiles(t, map[string]int{
		file1: 100,
	})

	type test struct {
		indexedLocations  []common.Location
		expectedLocations []common.Location
	}

	tests := []test{
		{ // full
			expectedLocations: []common.Location{
				{0, 100},
			},
		},
		{ // left indexed
			indexedLocations: []common.Location{
				{0, 50},
			},
			expectedLocations: []common.Location{
				{50, 100},
			},
		},
		{ // right indexed
			indexedLocations: []common.Location{
				{50, 100},
			},
			expectedLocations: []common.Location{
				{0, 50},
			},
		},
		{ // middle indexed
			indexedLocations: []common.Location{
				{50, 60},
			},
			expectedLocations: []common.Location{
				{0, 50},
				{60, 100},
			},
		},
		{ // multiple middle indexed
			indexedLocations: []common.Location{
				{0, 11},
				{50, 60},
				{88, 100},
			},
			expectedLocations: []common.Location{
				{11, 50},
				{60, 88},
			},
		},
	}

	for i := range tests {
		t.Run(fmt.Sprintf("test_util %d", i), func(t *testing.T) {
			_db.Exec("DELETE FROM file_segments") // cleanup the state
			for j := range tests[i].indexedLocations {
				_, err := sdb.CheckinSegment(file1Id, tests[i].indexedLocations[j], time.Now(), time.Now())
				require.NoError(t, err)
			}

			locs, err := SelectLocationsForIndexing(dbc, file1)
			require.NoError(t, err)
			require.Equal(t, tests[i].expectedLocations, locs)
		})
	}

}

func PopulateFiles(t *testing.T, spec map[string]int) {
	for path, size := range spec {
		payload := bytes.Repeat([]byte("A"), size)
		err := os.WriteFile(path, payload, os.ModePerm)
		require.NoError(t, err)
	}
}

func PrepareSegmentsForTesting(t *testing.T) (*sql.DB, *db.SegmentsDb, *db.FilesDb, string) {
	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	_db, err := db.OpenDb(storageRoot, 100)
	require.NoError(t, err)

	fdb := db.NewFilesDb(_db)
	sdb := db.NewSegmentsDb(_db)

	return _db, sdb, fdb, storageRoot
}
