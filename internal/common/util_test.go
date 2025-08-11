package common

import (
	"slices"
	"testing"
)

func TestChunksN(t *testing.T) {
	tests := []struct {
		name   string
		items  []int
		chunks int
		want   [][]int
	}{
		{
			name:   "empty slice",
			items:  []int{},
			chunks: 3,
			want:   [][]int{nil, nil, nil},
		},
		{
			name:   "full slice",
			items:  []int{1, 2},
			chunks: 2,
			want:   [][]int{{1}, {2}},
		},
		{
			name:   "equal chunks",
			items:  []int{1, 2, 3, 4},
			chunks: 2,
			want:   [][]int{{1, 2}, {3, 4}},
		},
		{
			name:   "chunks with remainder",
			items:  []int{1, 2, 3, 4, 5},
			chunks: 3,
			want:   [][]int{{1, 2}, {3, 4}, {5}},
		},
		{
			name:   "invalid chunk count",
			items:  []int{1, 2, 3},
			chunks: 0,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got := ChunksN(tt.items, tt.chunks)
				if !slices.EqualFunc(
					got, tt.want, func(i1 []int, i2 []int) bool {
						return slices.Compare(i1, i2) == 0
					},
				) {
					t.Errorf("ChunksN() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}
