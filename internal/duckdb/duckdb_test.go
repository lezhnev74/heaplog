package duckdb

import (
	"cmp"
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
)

type putSegmentTestCase struct {
	name  string
	input map[string][][]common.Message
}

func TestPutSegment(t *testing.T) {
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
		},
		{
			name:  "empty",
			input: map[string][][]common.Message{},
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
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				ctx := context.Background()
				db, err := NewDuckDB(ctx, "")
				require.NoError(t, err)
				err = db.Migrate()
				require.NoError(t, err)

				segmentIds := []int(nil)
				for path, segments := range tt.input {
					for _, segment := range segments {
						segmentId, err := db.PutSegment(path, nil, segment)
						require.NoError(t, err)
						segmentIds = append(segmentIds, segmentId)
					}
				}

				messagesSeq, err := db.GetMessages(segmentIds)
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

				require.Equal(t, expectedMessages, messages)
			},
		)
	}

}
