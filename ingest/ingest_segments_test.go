package ingest

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"heaplog/common"
	"heaplog/test"
	"os"
	"path"
	"slices"
	"testing"
	"time"
)

// This test is integrational, it involves files IO, duckdb storage,
// Inverted index + an indexer working together.
// Preparation for the test is massive, all services must be configured beforehand.

func TestIngestErrors(t *testing.T) {
	// we should expect errors related to file I/O in the first place.
	// ingestion will be the first who discovers that a file is no longer available
	// (as it won't be able to open it). It should blacklist such file and skip it.
	// There is another independent logic that crawls files and detects removals.

	// 1. Check in missing file: ingestion won't happen. It will log an error and does nothing.
	segmentSize := int64(100_000)
	storage, _indexer, _ := test.PrepareServices(t, segmentSize)
	ingestor := NewIngestor(storage, _indexer, segmentSize, 1)
	missingFiles := make(map[string]int64)
	missingFiles["/tmp/file_missing"] = 1000
	storage.CheckInFiles(missingFiles)

	err := ingestor.Ingest()
	require.NoError(t, err)

	// 2. Check a shrunk file
	storage, _indexer, _ = test.PrepareServices(t, segmentSize)
	ingestor = NewIngestor(storage, _indexer, segmentSize, 1)
	logFiles := test.PrepareDataSourceFiles(t)

	for file, size := range logFiles {
		logFiles[file] = size * 2 // report 2x size (imitate shrinking)

		// imitate indexed segments, so reading will start after them in the Stream
		terms := []string{"term1"}
		storage.CheckInSegment(common.IndexedSegment{
			common.HashFile(file),
			[]common.IndexedMessage{
				{
					Loc:  common.Location{0, size + 10},
					Date: time.Now(),
				},
			},
		}, terms)
	}
	storage.CheckInFiles(logFiles)

	ingestor = NewIngestor(storage, _indexer, 50, 1)
	err = ingestor.Ingest()
	require.NoError(t, err)
}

func TestIngestMultipleSegments(t *testing.T) {

	s, _indexer, _ := test.PrepareServices(t, 10)
	ingestor := NewIngestor(
		s,
		_indexer,
		10, // small segment size means each segment will only contains one message
		1,
	)
	logFiles := test.PrepareDataSourceFiles(t)

	// Test plan:
	// 0. Put the file to the storage
	// 1. Call ingestor.Ingest() to make a single cycle
	// 2. Assert indexed segments in the storage

	// Exec:
	// 0. Put the file to the storage
	filePaths := maps.Keys(logFiles)
	slices.Sort(filePaths)
	_, _, err := s.CheckInFiles(logFiles)
	require.NoError(t, err)

	// 1. Call ingestor.Ingest() to make a single cycle
	err = ingestor.Ingest()
	require.NoError(t, err)

	// 2. assert indexed segments in the storage
	// every segment is a separate message:
	expectedFileLocations := map[string][]common.Location{
		filePaths[0]: {{0, 43}, {43, 87}, {87, 134}, {134, 177}},
		filePaths[1]: {{0, 42}, {42, 82}, {82, 135}, {135, 178}},
	}

	for _, filePath := range filePaths {
		expectedLocations := expectedFileLocations[filePath]
		actualLocations, err := s.ReadIndexedLocations(common.HashFile(filePath))
		require.NoError(t, err)
		require.EqualValues(t, expectedLocations, actualLocations)
	}
}

func TestIngestIncompleteFile(t *testing.T) {

	// Test notes:
	// a growing file gets new data constantly.
	// at a moment a message can be partially present at the end of the file.
	// which means that indexing can happen faster that appending.
	// The system must be able to update messages (a message is identified by file+startPos, so the endpos may grow)

	s, _indexer, _ := test.PrepareServices(t, 10)
	ingestor := NewIngestor(
		s,
		_indexer,
		1000,
		1,
	)

	// Create the file
	contents := `[2023-01-05 23:40:20.779604] message first second third
[2023-01-05 23:45:11.324153] message forth fifth sixth seventh
`
	filesRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	dstFilePath := path.Join(filesRoot, "stream.txt")
	dstFile, err := os.OpenFile(dstFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	require.NoError(t, err)
	logFiles := map[string]int64{}

	// Checkin part of the file
	_, err = dstFile.WriteString(contents[:36])
	require.NoError(t, err)
	logFiles[dstFilePath] = 36
	_, _, err = s.CheckInFiles(logFiles)
	require.NoError(t, err)

	// Ingest once
	err = ingestor.Ingest()
	require.NoError(t, err)

	// Checkin full file
	_, err = dstFile.WriteString(contents[36:])
	require.NoError(t, err)
	logFiles[dstFilePath] = int64(len(contents)) // full file contents
	_, _, err = s.CheckInFiles(logFiles)
	require.NoError(t, err)

	// Ingest again
	err = ingestor.Ingest()
	require.NoError(t, err)
	err = ingestor.RescanTailMessages()
	require.NoError(t, err)

	// Assert indexed segments in the storage
	expectedLocations := []common.Location{{0, 56}, {56, 119}}
	actualLocations, err := s.ReadIndexedLocations(common.HashFile(dstFilePath))
	require.NoError(t, err)
	require.EqualValues(t, expectedLocations, actualLocations)
}
