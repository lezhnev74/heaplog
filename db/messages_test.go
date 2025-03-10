package db_test

import (
	"errors"
	"fmt"
	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/test_util"
	"os"
	"testing"

	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
)

func TestIterateMessages(t *testing.T) {
	// Messages are stored in a short format (only fromPos) is saved.
	// calculation of posTo is done at run-time
	// (either from the next Messages in the segment, or the segment posTo)

	// 1. Ingest Messages
	_db, storageRoot := test_util.PrepareTestDb(t)
	defer func() { _ = os.RemoveAll(storageRoot) }()
	ing, _ := test_util.PrepareTestIngest(t, 50, storageRoot, _db)

	// Populate files for tests
	sampleFile1 := `
[2024-07-30T00:00:04.000000+00:00] testing.Info: message
	multiline
[2024-07-30T00:00:05.111111+00:00] testing.DEBUG: message 2
`
	sampleFile2 := `
[2024-07-30T00:00:06.222222+00:00] testing.Info: message 3
multile
	possibly
		very long
 [2024-07-30T00:00:07.333333+00:00] <- could include dates too
[2024-07-30T00:00:08.444444+00:00] testing.DEBUG: message 4
`

	file1 := test_util.PopulateFile(storageRoot, []byte(sampleFile1))
	file2 := test_util.PopulateFile(storageRoot, []byte(sampleFile2))

	// Ingest data before testing search
	_, _, err := _db.CheckInFiles([]string{file1, file2})
	require.NoError(t, err)

	file1Id, _ := _db.GetFileId(file1)
	file2Id, _ := _db.GetFileId(file2)

	err = ing.Index([]string{file1, file2})
	require.NoError(t, err)

	_db.MessagesDb.Flush()

	type test struct {
		it               func() go_iterators.Iterator[db.Message]
		expectedMessages []db.Message
	}

	tests := []test{
		{ // Read All
			it: func() go_iterators.Iterator[db.Message] {
				it, err := _db.AllMessagesIt()
				require.NoError(t, err)
				return it
			},
			expectedMessages: []db.Message{
				{SegmentId: 1, Loc: common.Location{From: 1, To: 69}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file1Id},
				{SegmentId: 2, Loc: common.Location{From: 69, To: 129}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file1Id},
				{SegmentId: 3, Loc: common.Location{From: 1, To: 153}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id},
				{SegmentId: 4, Loc: common.Location{From: 153, To: 213}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id},
			},
		},
		{ // Read From file
			it: func() go_iterators.Iterator[db.Message] {
				it, err := _db.AllMessagesInFileIt(file2Id)
				require.NoError(t, err)
				return it
			},
			expectedMessages: []db.Message{
				{SegmentId: 3, Loc: common.Location{From: 1, To: 153}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id},
				{SegmentId: 4, Loc: common.Location{From: 153, To: 213}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id},
			},
		},
		{ // No File
			it: func() go_iterators.Iterator[db.Message] {
				it, err := _db.AllMessagesInFileIt(3)
				require.NoError(t, err)
				return it
			},
			expectedMessages: []db.Message{},
		},
		{ // Read Segments
			it: func() go_iterators.Iterator[db.Message] {
				it, err := _db.AllMessagesInSegmentsIt([]uint32{2, 4})
				require.NoError(t, err)
				return it
			},
			expectedMessages: []db.Message{
				{SegmentId: 2, Loc: common.Location{From: 69, To: 129}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file1Id},
				{SegmentId: 4, Loc: common.Location{From: 153, To: 213}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file2Id},
			},
		},
		{ // No segments
			it: func() go_iterators.Iterator[db.Message] {
				it, err := _db.AllMessagesInSegmentsIt([]uint32{99})
				require.NoError(t, err)
				return it
			},
			expectedMessages: []db.Message{},
		},
		{ // Half segments
			it: func() go_iterators.Iterator[db.Message] {
				it, err := _db.AllMessagesInSegmentsIt([]uint32{99, 2})
				require.NoError(t, err)
				return it
			},
			expectedMessages: []db.Message{
				{SegmentId: 2, Loc: common.Location{From: 69, To: 129}, RelDateLoc: common.Location{From: 1, To: 32}, FileId: file1Id},
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			it := tt.it()
			matchedMessages := []db.Message{}
			for {
				m, err := it.Next()
				if err != nil {
					if errors.Is(err, go_iterators.EmptyIterator) {
						break
					}
					require.NoError(t, err)
				}
				matchedMessages = append(matchedMessages, m)
			}

			require.Equal(t, tt.expectedMessages, matchedMessages)
		})
	}

}
