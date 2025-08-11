package ingest

import (
	"testing"

	"heaplog_2024/internal/common"
)

func Test_alignByLayouts(t *testing.T) {
	tests := []struct {
		name        string
		segmentSize int
		locs        []common.Location
		layouts     []common.Location
		want        [][]common.Location
	}{
		{
			name:        "empty inputs",
			segmentSize: 10,
			locs:        []common.Location{},
			layouts:     []common.Location{},
			want:        [][]common.Location{},
		},
		{
			name:        "overlapping layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.Location{
				{From: 0, To: 60},
				{From: 50, To: 80},
			},
			want: [][]common.Location{
				{{From: 0, To: 60}},
				{{From: 50, To: 80}},
			},
		},
		{
			name:        "zero-length layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.Location{
				{From: 10, To: 10},
				{From: 20, To: 20},
			},
			want: [][]common.Location{
				{{From: 10, To: 10}},
				{{From: 20, To: 20}},
			},
		},
		{
			name:        "layouts matching segment size",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.Location{
				{From: 0, To: 50},
				{From: 50, To: 100},
			},
			want: [][]common.Location{
				{{From: 0, To: 50}},
				{{From: 50, To: 100}},
			},
		},
		{
			name:        "layouts outside location range",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
			},
			layouts: []common.Location{
				{From: 60, To: 80},
				{From: 90, To: 100},
			},
			want: [][]common.Location{},
		},
		{
			name:        "single layout spanning multiple segments",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.Location{
				{From: 0, To: 90},
			},
			want: [][]common.Location{
				{{From: 0, To: 90}},
			},
		},
		{
			name:        "multiple locations with single layout",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
				{From: 60, To: 100},
			},
			layouts: []common.Location{
				{From: 30, To: 80},
			},
			want: [][]common.Location{
				{{From: 30, To: 80}},
			},
		},
		{
			name:        "single segment",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
			},
			layouts: []common.Location{
				{From: 0, To: 40},
				{From: 40, To: 50},
			},
			want: [][]common.Location{
				{
					{From: 0, To: 40},
					{From: 40, To: 50},
				},
			},
		},
		{
			name:        "multiple segments",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.Location{
				{From: 0, To: 30},
				{From: 30, To: 60},
				{From: 60, To: 90},
			},
			want: [][]common.Location{
				{
					{From: 0, To: 30},
					{From: 30, To: 60},
				},
				{
					{From: 60, To: 90},
				},
			},
		},
		{
			name:        "non-intersecting layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 50, To: 150},
			},
			layouts: []common.Location{
				{From: 0, To: 40},
				{From: 60, To: 100},
			},
			want: [][]common.Location{
				{
					{From: 60, To: 100},
				},
			},
		},
		{
			name:        "non-overlapping layouts should not be combined",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 150},
			},
			layouts: []common.Location{
				{From: 0, To: 40},
				{From: 60, To: 100},
			},
			want: [][]common.Location{
				{
					{From: 0, To: 40},
				},
				{
					{From: 60, To: 100},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got := alignByLayouts(tt.segmentSize, tt.locs, tt.layouts)
				if len(got) != len(tt.want) {
					t.Errorf("alignByLayouts() got = %v, want %v", got, tt.want)
				}
				for i := range got {
					if len(got[i]) != len(tt.want[i]) {
						t.Errorf("alignByLayouts() segment %d got = %v, want %v", i, got[i], tt.want[i])
					}
					for j := range got[i] {
						if got[i][j] != tt.want[i][j] {
							t.Errorf(
								"alignByLayouts() element [%d][%d] got = %v, want %v",
								i,
								j,
								got[i][j],
								tt.want[i][j],
							)
						}
					}
				}
			},
		)
	}
}
