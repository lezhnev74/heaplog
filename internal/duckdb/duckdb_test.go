package duckdb

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
)

type putSegmentTestCase struct {
	name     string
	path     string
	segments [][]common.Message
	want     []common.Message
}

func TestPutSegment(t *testing.T) {
	tests := []putSegmentTestCase{
		{
			name: "gap b/w segments",
			path: "path1",
			segments: [][]common.Message{
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
		},
		{
			name: "basic test",
			path: "path1",
			segments: [][]common.Message{
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
		},
		{
			name:     "empty",
			path:     "path1",
			segments: [][]common.Message(nil),
		},
		{
			name: "one entry",
			path: "path1",
			segments: [][]common.Message{
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
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				ctx := context.Background()
				db, err := NewDuckDB(ctx, "")
				require.NoError(t, err)
				err = db.Migrate()
				require.NoError(t, err)

				for _, segment := range tt.segments {
					require.NoError(t, db.PutSegment(tt.path, nil, segment))
				}

				messagesSeq, err := db.GetAllMessages(tt.path)
				require.NoError(t, err)
				messages := slices.Collect(messagesSeq)

				expectedMessages := []common.Message(nil)
				for _, segment := range tt.segments {
					expectedMessages = append(expectedMessages, segment...)
				}

				require.Equal(t, expectedMessages, messages)
			},
		)
	}

}
