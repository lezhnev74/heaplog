package ingest

import (
	"heaplog_2024/common"
	"heaplog_2024/db"

	"golang.org/x/xerrors"
)

// SelectLocationsForIndexing returns contiguous file runs that were never indexed.
// Excludes segments that were previously checked in.
func SelectLocationsForIndexing(_db *db.DbContainer, file string) ([]common.Location, error) {

	fileSize, err := common.FileSize(file)
	if err != nil {
		return nil, xerrors.Errorf("file %s: %w", file, err)
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

	unindexedLocations := []common.Location{{From: 0, To: fileSize}}
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
