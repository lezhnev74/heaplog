package ingest

import (
	"testing"

	"heaplog_2024/internal/common"
)

func TestSegmentLayoutsByLocations(t *testing.T) {
	tests := []struct {
		name        string
		segmentSize int
		locs        []common.Location
		layouts     []MessageLayout
		want        [][]MessageLayout
	}{
		{
			name:        "empty inputs",
			segmentSize: 10,
			locs:        []common.Location{},
			layouts:     []MessageLayout{},
			want:        [][]MessageLayout{},
		},
		{
			name:        "overlapping layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 60}},
				{Loc: common.Location{From: 50, To: 80}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 0, To: 60}}},
				{{Loc: common.Location{From: 50, To: 80}}},
			},
		},
		{
			name:        "zero-length layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 10, To: 10}},
				{Loc: common.Location{From: 20, To: 20}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 10, To: 10}}},
				{{Loc: common.Location{From: 20, To: 20}}},
			},
		},
		{
			name:        "layouts matching layouts size",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 50}},
				{Loc: common.Location{From: 50, To: 100}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 0, To: 50}}},
				{{Loc: common.Location{From: 50, To: 100}}},
			},
		},
		{
			name:        "layouts outside location range",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 60, To: 80}},
				{Loc: common.Location{From: 90, To: 100}},
			},
			want: [][]MessageLayout{},
		},
		{
			name:        "single layout spanning multiple segments",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 90}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 0, To: 90}}},
			},
		},
		{
			name:        "multiple locations with single layout",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
				{From: 60, To: 100},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 30, To: 80}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 30, To: 80}}},
			},
		},
		{
			name:        "single layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 40}},
				{Loc: common.Location{From: 40, To: 50}},
			},
			want: [][]MessageLayout{
				{
					{Loc: common.Location{From: 0, To: 40}},
					{Loc: common.Location{From: 40, To: 50}},
				},
			},
		},
		{
			name:        "multiple segments",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 30}},
				{Loc: common.Location{From: 30, To: 60}},
				{Loc: common.Location{From: 60, To: 90}},
			},
			want: [][]MessageLayout{
				{
					{Loc: common.Location{From: 0, To: 30}},
					{Loc: common.Location{From: 30, To: 60}},
				},
				{
					{Loc: common.Location{From: 60, To: 90}},
				},
			},
		},
		{
			name:        "non-intersecting layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 50, To: 150},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 40}},
				{Loc: common.Location{From: 60, To: 100}},
			},
			want: [][]MessageLayout{
				{
					{Loc: common.Location{From: 60, To: 100}},
				},
			},
		},
		{
			name:        "non-overlapping layouts should not be combined",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 150},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 40}},
				{Loc: common.Location{From: 60, To: 100}},
			},
			want: [][]MessageLayout{
				{
					{Loc: common.Location{From: 0, To: 40}},
				},
				{
					{Loc: common.Location{From: 60, To: 100}},
				},
			},
		},
		{
			name:        "non overlapping layouts not abutting",
			segmentSize: 100,
			locs: []common.Location{
				{From: 40, To: 50},
				{From: 60, To: 70},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 30, To: 45}},
				{Loc: common.Location{From: 55, To: 65}},
			},
			want: [][]MessageLayout{
				{
					{Loc: common.Location{From: 30, To: 45}},
				},
				{
					{Loc: common.Location{From: 55, To: 65}},
				},
			},
		},
		{
			name:        "left from the locs",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 5}},
			},
			want: [][]MessageLayout{},
		},
		{
			name:        "right from the locs",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 30, To: 35}},
			},
			want: [][]MessageLayout{},
		},
		{
			name:        "partial overlap left",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 0, To: 11}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 0, To: 11}}},
			},
		},
		{
			name:        "partial overlap right",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []MessageLayout{
				{Loc: common.Location{From: 16, To: 30}},
			},
			want: [][]MessageLayout{
				{{Loc: common.Location{From: 16, To: 30}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got := segmentLayoutsByLocations(tt.segmentSize, tt.locs, tt.layouts)
				if len(got) != len(tt.want) {
					t.Errorf("alignByLayouts() got = %v, want %v", got, tt.want)
				}
				for i := range got {
					if len(got[i]) != len(tt.want[i]) {
						t.Errorf("alignByLayouts() layouts %d got = %v, want %v", i, got[i], tt.want[i])
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
