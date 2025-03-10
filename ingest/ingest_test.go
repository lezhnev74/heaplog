package ingest_test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/test_util"
	"os"
	"testing"
	"time"
)

func TestSmallSegmentsMerging(t *testing.T) {
	messages := [][]byte{
		[]byte("[2023-01-05T23:40:20.779604+00:00] message 1\n"), // 45 bytes
		[]byte("[2023-01-05T23:40:20.779604+00:00] message 2\n"), // 45 bytes
		[]byte("[2023-01-05T23:40:20.779604+00:00] message 3\n"), // 45 bytes
	}

	_db, storageRoot := test_util.PrepareTestDb(t)
	defer os.RemoveAll(storageRoot)
	ing, _ := test_util.PrepareTestIngest(t, 80 /*2 messages*/, storageRoot, _db)

	file := test_util.PopulateFile(storageRoot, messages[0])
	_db.CheckInFiles([]string{file})
	fileId, _ := _db.GetFileId(file)

	err := ing.Index([]string{file})
	require.NoError(t, err)

	// Append another message and index
	for i := 1; i < len(messages); i++ {
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f.Write(messages[i])
		require.NoError(t, err)
		require.NoError(t, f.Close())

		err = ing.Index([]string{file})
		require.NoError(t, err)
	}

	// make sure all messages are visible
	expectedMessages := []db.Message{
		{SegmentId: 1, Loc: common.Location{From: 0, To: 45}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: fileId, Date: nil},  // SAME SEGMENT
		{SegmentId: 1, Loc: common.Location{From: 45, To: 90}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: fileId, Date: nil}, // SAME SEGMENT
		{SegmentId: 2, Loc: common.Location{From: 90, To: 135}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: fileId, Date: nil},
	}
	_db.MessagesDb.Flush()
	actualMessages, err := _db.AllMessages(fileId)
	require.NoError(t, err)
	require.Equal(t, expectedMessages, actualMessages)
}

func TestMessageDetectionWithDifferentSegmentSize(t *testing.T) {
	sourceStream := []byte(`
[2023-01-05T23:40:20.779604+00:00] testing.Info: message 
multile
	possibly
		very long
 [2024-01-05T23:40:20.999999+00:00] <- could include dates too
[2023-01-05T23:42:00.213212+00:00] testing.DEBUG: message 2
`)

	type test struct {
		segmentSize      int64
		expectedMessages []db.Message
	}
	expectedMessages := []db.Message{
		{
			SegmentId:  0, // do not compare
			Loc:        common.Location{From: 1, To: 152},
			RelDateLoc: common.Location{From: 1, To: 32},
		},
		{
			SegmentId:  0, // do not compare
			Loc:        common.Location{From: 152, To: 212},
			RelDateLoc: common.Location{From: 1, To: 32},
		},
	}

	// check all possible segment sizes
	for segmentSize := 1; segmentSize < len(sourceStream)+1; segmentSize++ {
		t.Run(fmt.Sprintf("test_util %d", segmentSize), func(t *testing.T) {
			_db, storageRoot := test_util.PrepareTestDb(t)
			defer os.RemoveAll(storageRoot)
			ing, _ := test_util.PrepareTestIngest(t, uint64(segmentSize), storageRoot, _db)

			file := test_util.PopulateFile(storageRoot, sourceStream)
			_db.CheckInFiles([]string{file})
			fileId, _ := _db.GetFileId(file)

			err := ing.Index([]string{file})
			require.NoError(t, err)

			_db.MessagesDb.Flush()
			time.Sleep(time.Millisecond)

			messages, err := _db.AllMessages(fileId)
			require.NoError(t, err)

			for i := range messages {
				messages[i].SegmentId = 0 // do not compare
				messages[i].FileId = 0    // do not compare
			}

			require.Equal(t, expectedMessages, messages)
		})
	}
}
