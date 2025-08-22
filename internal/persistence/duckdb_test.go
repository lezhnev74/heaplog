package persistence

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal"
	"heaplog_2024/internal/common"
)

type putSegmentTestCase struct {
	name             string
	input            map[string][][]common.Message
	minDate          *time.Time
	maxDate          *time.Time
	expectedMessages []int // indexes within all messages
}

func TestWipeFiles(t *testing.T) {
	ctx := context.Background()
	logger, err := internal.NewLogger("test")
	require.NoError(t, err)
	db, err := NewDuckDB(ctx, "", logger)
	require.NoError(t, err)

	// Insert some data
	_, err = db.PutSegment(
		"path1", []common.Message{
			{
				MessageLayout: common.MessageLayout{
					Loc:     common.Location{From: 0, To: 10},
					DateLoc: common.Location{From: 1, To: 2},
				},
				Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
			},
		},
	)
	require.NoError(t, err)

	// Wipe files
	err = db.WipeFile("path1")
	require.NoError(t, err)

	// Check that the file is gone
	files, err := db.GetSegments()
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestWipeSegments(t *testing.T) {
	ctx := context.Background()
	logger, err := internal.NewLogger("test")
	require.NoError(t, err)
	db, err := NewDuckDB(ctx, "", logger)
	require.NoError(t, err)

	// Insert some data
	_, err = db.PutSegment(
		"path1", []common.Message{
			{
				MessageLayout: common.MessageLayout{
					Loc:     common.Location{From: 0, To: 10},
					DateLoc: common.Location{From: 1, To: 2},
				},
				Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
			},
		},
	)
	require.NoError(t, err)

	// Wipe files
	_, err = db.WipeSegments("path1")
	require.NoError(t, err)

	// Check that the file is gone
	files, err := db.GetSegments()
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestWipeSegment(t *testing.T) {
	ctx := context.Background()
	logger, err := internal.NewLogger("test")
	require.NoError(t, err)
	db, err := NewDuckDB(ctx, "", logger)
	require.NoError(t, err)

	// Insert some data
	_, err = db.PutSegment(
		"path1", []common.Message{
			{
				MessageLayout: common.MessageLayout{
					Loc:     common.Location{From: 0, To: 10},
					DateLoc: common.Location{From: 1, To: 2},
				},
				Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
			},
		},
	)
	_, err = db.PutSegment(
		"path1", []common.Message{
			{
				MessageLayout: common.MessageLayout{
					Loc:     common.Location{From: 20, To: 30},
					DateLoc: common.Location{From: 21, To: 22},
				},
				Date: common.MakeTimeV("2024-01-02T00:00:00.000000+00:00"),
			},
		},
	)
	require.NoError(t, err)

	// Wipe files
	_, err = db.WipeSegment("path1", common.Location{From: 20, To: 30})
	require.NoError(t, err)

	// Check only one segment remains
	files, err := db.GetSegments()
	require.NoError(t, err)

	expectedResult := map[string][]common.Location{
		"path1": []common.Location{
			{From: 0, To: 10},
		},
	}
	require.Equal(t, expectedResult, files)
}

func TestGetMessages(t *testing.T) {
	tests := []putSegmentTestCase{
		{
			name: "gap b/w segments",
			input: map[string][][]common.Message{
				"path1": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 10, To: 20},
								DateLoc: common.Location{From: 11, To: 12},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:01.000000+00:00"),
						},
					},
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 30, To: 40},
								DateLoc: common.Location{From: 31, To: 32},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:02.000000+00:00"),
						},
					},
				},
				"path2": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
						},
					},
				},
			},
			minDate: common.MakeTimeP("2024-01-01T00:00:00.000000+00:00"),
			maxDate: common.MakeTimeP("2024-01-01T00:00:02.000000+00:00"),
		},
		{
			name: "basic test",
			input: map[string][][]common.Message{
				"path1": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
						},
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 10, To: 20},
								DateLoc: common.Location{From: 11, To: 12},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:01.000000+00:00"),
						},
					},
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 20, To: 30},
								DateLoc: common.Location{From: 21, To: 22},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:02.000000+00:00"),
						},
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 30, To: 40},
								DateLoc: common.Location{From: 31, To: 32},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:03.000000+00:00"),
						},
					},
				},
				"path2": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-02T00:00:00.000000+00:00"),
						},
					},
				},
			},
			minDate: common.MakeTimeP("2024-01-01T00:00:00.000000+00:00"),
			maxDate: common.MakeTimeP("2024-01-02T00:00:00.000000+00:00"),
		},
		{
			name:    "empty",
			input:   map[string][][]common.Message{},
			minDate: nil,
			maxDate: nil,
		},
		{
			name: "one entry",
			input: map[string][][]common.Message{
				"path1": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
						},
					},
				},
			},
			minDate: common.MakeTimeP("2024-01-01T00:00:00.000000+00:00"),
			maxDate: common.MakeTimeP("2024-01-01T00:00:00.000000+00:00"),
		},
		{
			name: "date is before minDate",
			input: map[string][][]common.Message{
				"path1": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
						},
					},
				},
			},
			minDate:          common.MakeTimeP("2024-01-11T00:00:00.000000+00:00"),
			expectedMessages: []int{},
		},
		{
			name: "date is after maxDate",
			input: map[string][][]common.Message{
				"path1": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
						},
					},
				},
			},
			maxDate:          common.MakeTimeP("2023-01-01T00:00:00.000000+00:00"),
			expectedMessages: []int{},
		},
		{
			name: "date between",
			input: map[string][][]common.Message{
				"path1": {
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-01T00:00:00.000000+00:00"),
						},
					},
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-02T00:00:00.000000+00:00"),
						},
					},
					{
						common.Message{
							MessageLayout: common.MessageLayout{
								Loc:     common.Location{From: 0, To: 10},
								DateLoc: common.Location{From: 1, To: 2},
							},
							Date: common.MakeTimeV("2024-01-03T00:00:00.000000+00:00"),
						},
					},
				},
			},
			minDate:          common.MakeTimeP("2024-01-02T00:00:00.000000+00:00"),
			maxDate:          common.MakeTimeP("2024-01-02T23:00:00.000000+00:00"),
			expectedMessages: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				ctx := context.Background()
				logger, err := internal.NewLogger("test")
				require.NoError(t, err)
				db, err := NewDuckDB(ctx, "", logger)
				require.NoError(t, err)

				segmentIds := []int(nil)
				for path, segments := range tt.input {
					for _, segment := range segments {
						segmentId, err := db.PutSegment(path, segment)
						require.NoError(t, err)
						segmentIds = append(segmentIds, segmentId)
					}
				}

				messagesSeq, err := db.GetMessages(segmentIds, tt.minDate, tt.maxDate)
				require.NoError(t, err)
				messages := slices.Collect(messagesSeq)

				expectedMessages := []common.FileMessage(nil)
				for path, segments := range tt.input {
					for _, segment := range segments {
						for _, msg := range segment {
							expectedMessages = append(expectedMessages, common.FileMessage{File: path, Message: msg})
						}
					}
				}
				slices.SortFunc(
					expectedMessages, func(a, b common.FileMessage) int {
						return cmp.Compare(a.Date.UnixMicro(), b.Date.UnixMicro())
					},
				)
				if tt.expectedMessages != nil {
					filteredExpectedMessages := make([]common.FileMessage, 0)
					for i := range expectedMessages {
						if slices.Contains(tt.expectedMessages, i) {
							filteredExpectedMessages = append(filteredExpectedMessages, expectedMessages[i])
						}
					}
					expectedMessages = filteredExpectedMessages
				}

				require.Equal(t, len(expectedMessages), len(messages))
				if len(expectedMessages) > 0 {
					require.Equal(t, expectedMessages, messages)
				}
			},
		)
	}

}
