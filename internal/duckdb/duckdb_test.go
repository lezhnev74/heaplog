package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
)

func TestDuckDB_PutAndGetSegments(t *testing.T) {
	ctx := context.Background()
	db, err := NewDuckDB(ctx, "")
	require.NoError(t, err)
	err = db.Migrate()
	require.NoError(t, err)

	testData := map[string][][]common.Message{
		"path1": {
			// segment 1
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
			// segment 2
			{
				common.Message{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 30, To: 40},
						DateLoc: common.Location{From: 31, To: 32},
					},
					Date: common.MakeTimeV("2024-01-01T00:00:02.000000+00:00"),
				},
				common.Message{
					MessageLayout: common.MessageLayout{
						Loc:     common.Location{From: 40, To: 50},
						DateLoc: common.Location{From: 41, To: 42},
					},
					Date: common.MakeTimeV("2024-01-01T00:00:03.000000+00:00"),
				},
			},
		},
	}

	for f, segments := range testData {
		for _, segment := range segments {
			require.NoError(t, db.PutSegment(f, nil, segment))
		}
	}
}
