package ingest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"heaplog/storage"
	"os"
	"path"
	"slices"
	"testing"
	"time"
)

func TestItDiscoversFiles(t *testing.T) {
	// Test plan:
	// 1. Put 2 files in the folder
	// 2. Call "discover"
	// 3. See the files
	// 4. Remove one file, add another
	// 5. Call again "discover"
	// 6. See the files

	// Exec:
	// 1. Put 2 files in the folder
	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	storage, err := storage.NewStorage(storageRoot, time.Second)
	require.NoError(t, err)

	files := []string{
		path.Join(storageRoot, "file1.log"),
		path.Join(storageRoot, "file2.log"),
	}

	for _, file := range files {
		require.NoError(t, os.WriteFile(file, []byte("any payload"), os.ModePerm))
	}

	// 2. Call "discover"
	discovery := NewDiscover(
		[]string{fmt.Sprintf("%s/*.log", storageRoot)}, // glob
		storage,
	)
	require.NoError(t, discovery.DiscoverFiles())

	// 3. See the files
	actualFiles, err := storage.AllFiles()
	require.NoError(t, err)

	actualFilenames := maps.Keys(actualFiles)
	slices.Sort(files)
	slices.Sort(actualFilenames)

	require.EqualValues(t, files, actualFilenames)

	// 4. Remove one file, add another
	require.NoError(t, os.Remove(files[0]))
	files[0] = path.Join(storageRoot, "file3.log")
	require.NoError(t, os.WriteFile(files[0], []byte("any payload"), os.ModePerm))

	// 5. Call again "discover"
	require.NoError(t, discovery.DiscoverFiles())
	require.NoError(t, discovery.DiscoverFiles()) // + idempotency test

	// 6. See the files
	actualFiles, err = storage.AllFiles()
	require.NoError(t, err)

	actualFilenames = maps.Keys(actualFiles)
	slices.Sort(files)
	slices.Sort(actualFilenames)

	require.EqualValues(t, files, actualFilenames)
}
