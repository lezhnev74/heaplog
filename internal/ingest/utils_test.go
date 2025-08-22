package ingest

import (
	"slices"
	"testing"

	"heaplog_2024/internal/common"
)

func TestFilesWithIncompleteTrailingSegments(t *testing.T) {
	tests := []struct {
		name            string
		segmentLen      int
		indexedSegments map[string][]common.Location
		accessibleFiles map[string]int
		want            []string
	}{
		{
			name:            "empty inputs",
			segmentLen:      100,
			indexedSegments: map[string][]common.Location{},
			accessibleFiles: map[string]int{},
			want:            []string{},
		},
		{
			name:       "complete segment",
			segmentLen: 100,
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 100}},
			},
			accessibleFiles: map[string]int{
				"file1": 100,
			},
			want: []string{},
		},
		{
			name:       "incomplete trailing segment at file end",
			segmentLen: 100,
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 80}},
			},
			accessibleFiles: map[string]int{
				"file1": 80,
			},
			want: []string{},
		},
		{
			name:       "incomplete trailing segment not at file end",
			segmentLen: 100,
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 80}},
			},
			accessibleFiles: map[string]int{
				"file1": 120,
			},
			want: []string{"file1"},
		},
		{
			name:       "full trailing segment not at file end",
			segmentLen: 80,
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 80}},
			},
			accessibleFiles: map[string]int{
				"file1": 120,
			},
			want: []string{},
		},
		{
			name:       "multiple files one incomplete",
			segmentLen: 100,
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 100}},
				"file2": {{From: 0, To: 80}},
			},
			accessibleFiles: map[string]int{
				"file1": 100,
				"file2": 120,
			},
			want: []string{"file2"},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				var got []string
				for v := range filesWithIncompleteTrailingSegments(
					tt.segmentLen,
					tt.indexedSegments,
					tt.accessibleFiles,
				) {
					got = append(got, v)
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("filesWithIncompleteTrailingSegments() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestFindMisalignedSegments(t *testing.T) {
	tests := []struct {
		name              string
		indexedSegments   map[string][]common.Location
		foundFilesLayouts map[string][]common.MessageLayout
		want              []string
	}{
		{
			name:              "empty inputs",
			indexedSegments:   map[string][]common.Location{},
			foundFilesLayouts: map[string][]common.MessageLayout{},
			want:              []string{},
		},
		{
			name: "aligned segments",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {{Loc: common.Location{From: 0, To: 10}}},
			},
			want: []string{},
		},
		{
			name: "misaligned start",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {{Loc: common.Location{From: 5, To: 10}}},
			},
			want: []string{"file1"},
		},
		{
			name: "misaligned end",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {{Loc: common.Location{From: 0, To: 15}}},
			},
			want: []string{"file1"},
		},
		{
			name: "incomplete index",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {
					{Loc: common.Location{From: 0, To: 5}},
					{Loc: common.Location{From: 5, To: 9}},
				},
			},
			want: []string{"file1"},
		},
		{
			name: "unindexed",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {},
			},
			want: []string{"file1"},
		},
		{
			name: "multiple files one misaligned",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}},
				"file2": {{From: 0, To: 10}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {{Loc: common.Location{From: 0, To: 10}}},
				"file2": {{Loc: common.Location{From: 5, To: 15}}},
			},
			want: []string{"file2"},
		},
		{
			name: "reports once per file",
			indexedSegments: map[string][]common.Location{
				"file1": {{From: 0, To: 10}, {From: 10, To: 20}},
				"file2": {{From: 0, To: 10}, {From: 10, To: 20}},
			},
			foundFilesLayouts: map[string][]common.MessageLayout{
				"file1": {
					{Loc: common.Location{From: 0, To: 9}},
					{Loc: common.Location{From: 12, To: 15}},
				},
				"file2": {
					{Loc: common.Location{From: 5, To: 15}},
					{Loc: common.Location{From: 16, To: 17}},
				},
			},
			want: []string{"file1", "file2"},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				var got []string
				for v := range findMisalignedSegments(tt.indexedSegments, tt.foundFilesLayouts) {
					got = append(got, v)
				}
				slices.Sort(got)
				slices.Sort(tt.want)
				if !slices.Equal(got, tt.want) {
					t.Errorf("findMisalignedSegments() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestSegmentLayoutsByLocations(t *testing.T) {
	tests := []struct {
		name        string
		segmentSize int
		locs        []common.Location
		layouts     []common.MessageLayout
		want        [][]common.MessageLayout
	}{
		{
			name:        "empty inputs",
			segmentSize: 10,
			locs:        []common.Location{},
			layouts:     []common.MessageLayout{},
			want:        [][]common.MessageLayout{},
		},
		{
			name:        "overlapping layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 60}},
				{Loc: common.Location{From: 50, To: 80}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 10, To: 10}},
				{Loc: common.Location{From: 20, To: 20}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 50}},
				{Loc: common.Location{From: 50, To: 100}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 60, To: 80}},
				{Loc: common.Location{From: 90, To: 100}},
			},
			want: [][]common.MessageLayout{},
		},
		{
			name:        "single layout spanning multiple segments",
			segmentSize: 50,
			locs: []common.Location{
				{From: 0, To: 100},
			},
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 90}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 30, To: 80}},
			},
			want: [][]common.MessageLayout{
				{{Loc: common.Location{From: 30, To: 80}}},
			},
		},
		{
			name:        "single layouts",
			segmentSize: 100,
			locs: []common.Location{
				{From: 0, To: 50},
			},
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 40}},
				{Loc: common.Location{From: 40, To: 50}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 30}},
				{Loc: common.Location{From: 30, To: 60}},
				{Loc: common.Location{From: 60, To: 90}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 40}},
				{Loc: common.Location{From: 60, To: 100}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 40}},
				{Loc: common.Location{From: 60, To: 100}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 30, To: 45}},
				{Loc: common.Location{From: 55, To: 65}},
			},
			want: [][]common.MessageLayout{
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
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 5}},
			},
			want: [][]common.MessageLayout{},
		},
		{
			name:        "right from the locs",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 30, To: 35}},
			},
			want: [][]common.MessageLayout{},
		},
		{
			name:        "partial overlap left",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 0, To: 11}},
			},
			want: [][]common.MessageLayout{
				{{Loc: common.Location{From: 0, To: 11}}},
			},
		},
		{
			name:        "partial overlap right",
			segmentSize: 100,
			locs: []common.Location{
				{From: 10, To: 20},
			},
			layouts: []common.MessageLayout{
				{Loc: common.Location{From: 16, To: 30}},
			},
			want: [][]common.MessageLayout{
				{{Loc: common.Location{From: 16, To: 30}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got := alignSegmentsByMessageBoundaries(tt.segmentSize, tt.locs, tt.layouts)
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
