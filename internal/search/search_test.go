package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
)

func TestReadMessages(t *testing.T) {
	dir := t.TempDir()
	testFile1 := filepath.Join(dir, "test1.log")
	testFile2 := filepath.Join(dir, "test2.log")
	err := common.PopulateFiles(
		map[string][]byte{
			testFile1: []byte("ABCD"),
			testFile2: []byte("1234"),
		},
	)
	require.NoError(t, err)
	defer func() { _ = os.Remove(testFile1) }()
	defer func() { _ = os.Remove(testFile2) }()

	type test struct {
		messages         []common.FileMessage
		expectedMessages []common.FileMessageBody
	}

	tests := []test{
		{
			// empty
			messages:         []common.FileMessage{},
			expectedMessages: []common.FileMessageBody{},
		},
		{
			// one message in one file
			messages: []common.FileMessage{
				{
					File: testFile1,
					Message: common.Message{MessageLayout: common.MessageLayout{
						Loc:     common.Location{0, 2},
						DateLoc: common.Location{0, 1},
					}},
				},
			},
			expectedMessages: []common.FileMessageBody{
				{
					FileMessage: common.FileMessage{
						File: testFile1,
						Message: common.Message{MessageLayout: common.MessageLayout{
							Loc:     common.Location{0, 2},
							DateLoc: common.Location{0, 1},
						}},
					},
					Body: []byte("AB"),
				},
			},
		},
		{
			// multiple messages in multiple files
			messages: []common.FileMessage{
				{
					File: testFile1,
					Message: common.Message{MessageLayout: common.MessageLayout{
						Loc:     common.Location{0, 2},
						DateLoc: common.Location{0, 1},
					}},
				},
				{
					File: testFile1,
					Message: common.Message{MessageLayout: common.MessageLayout{
						Loc:     common.Location{2, 3},
						DateLoc: common.Location{2, 3},
					}},
				},
				{
					File: testFile2,
					Message: common.Message{MessageLayout: common.MessageLayout{
						Loc:     common.Location{3, 4},
						DateLoc: common.Location{3, 4},
					}},
				},
			},
			expectedMessages: []common.FileMessageBody{
				{
					FileMessage: common.FileMessage{
						File: testFile1,
						Message: common.Message{MessageLayout: common.MessageLayout{
							Loc:     common.Location{0, 2},
							DateLoc: common.Location{0, 1},
						}},
					},
					Body: []byte("AB"),
				},
				{
					FileMessage: common.FileMessage{
						File: testFile1,
						Message: common.Message{MessageLayout: common.MessageLayout{
							Loc:     common.Location{2, 3},
							DateLoc: common.Location{2, 3},
						}},
					},
					Body: []byte("C"),
				},
				{
					FileMessage: common.FileMessage{
						File: testFile2,
						Message: common.Message{MessageLayout: common.MessageLayout{
							Loc:     common.Location{3, 4},
							DateLoc: common.Location{3, 4},
						}},
					},
					Body: []byte("4"),
				},
			},
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("test %d", i), func(t *testing.T) {
				fullMessages := common.ReadMessages(context.Background(), slices.Values(tt.messages))
				actualMessages := make([]common.FileMessageBody, 0)
				for msg, err := range fullMessages {
					require.NoError(t, err)
					actualMessages = append(actualMessages, msg)
				}
				require.Equal(t, tt.expectedMessages, actualMessages)
			},
		)
	}
}
