package ingest

import (
	"errors"
	"golang.org/x/xerrors"
	"heaplog_2024/common"
	"heaplog_2024/db"
)

// SelectLocationsForIndexing returns contiguous file runs that were never indexed.
// Excludes segments that were previously checked in.
func SelectLocationsForIndexing(_db *db.DbContainer, file string) ([]common.Location, error) {

	fileSize, err := common.FileSize(file)
	if errors.Is(err, db.ErrNoData) {
		return nil, xerrors.Errorf("file %s is not indexed: %w", file, err)
	}

	fileId, err := _db.GetFileId(file)
	if err != nil {
		err = xerrors.Errorf("get file: %w", err)
		return nil, err
	}

	indexedLocations, err := _db.ReadIndexedLocations(fileId)
	if err != nil {
		return nil, err
	}

	unindexedLocations := []common.Location{{0, fileSize}}
	for _, indexedLocation := range indexedLocations {
		nextPending := make([]common.Location, 0, len(unindexedLocations))
		for _, pendingLocation := range unindexedLocations {
			nextPending = append(nextPending, pendingLocation.Remove(indexedLocation)...)
		}
		unindexedLocations = nextPending
	}

	// Merge siblings to make contiguous locations
	unindexedLocations = common.MergeLocations(unindexedLocations)

	return unindexedLocations, nil
}
