package ingest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog_2024/db"
	"os"
	"path"
	"slices"
	"testing"
)

func TestItDiscoversFiles(t *testing.T) {
	// Test plan:
	// 1. Put 2 files in the folder
	// 2. Call "discover"
	// 3. See the files
	// 4. RemoveQuery one file, add another
	// 5. Call again "discover"
	// 6. See the files

	// Exec:
	// 1. Put 2 files in the folder
	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(storageRoot) }()

	_db, err := db.OpenDb(storageRoot, 100)
	require.NoError(t, err)
	defer _db.Close()

	_storage := db.NewFilesDb(_db)

	files := []string{
		path.Join(storageRoot, "file1.log"),
		path.Join(storageRoot, "file2.log"),
	}

	for _, file := range files {
		err := os.WriteFile(file, []byte("any payload"), os.ModePerm)
		require.NoError(t, err)
	}

	// 2. Call "discover"
	discovery := NewDiscover(
		[]string{fmt.Sprintf("%s/*.log", storageRoot)}, // glob
		_storage,
	)
	_, _, err = discovery.DiscoverFiles()
	require.NoError(t, err)

	// 3. See the files
	actualFiles, err := _storage.AllFiles()
	require.NoError(t, err)

	slices.Sort(files)
	slices.Sort(actualFiles)

	require.EqualValues(t, files, actualFiles)

	// 4. RemoveQuery one file, add another
	require.NoError(t, os.Remove(files[0]))
	files[0] = path.Join(storageRoot, "file3.log")
	require.NoError(t, os.WriteFile(files[0], []byte("any payload"), os.ModePerm))

	// 5. Call again "discover"
	_, _, err = discovery.DiscoverFiles()
	require.NoError(t, err)
	_, _, err = discovery.DiscoverFiles() // + idempotency test_util
	require.NoError(t, err)

	// 6. See the files
	actualFiles, err = _storage.AllFiles()
	require.NoError(t, err)

	slices.Sort(files)
	slices.Sort(actualFiles)

	require.EqualValues(t, files, actualFiles)
}
