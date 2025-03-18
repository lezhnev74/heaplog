package common

import (
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeqBatchGroup(t *testing.T) {
	type test struct {
		in       iter.Seq[int]
		g        func(int) int
		expected [][]int
	}

	tests := []test{
		{
			in:       slices.Values([]int{}),
			g:        func(i int) int { return i / 10 },
			expected: [][]int(nil),
		},
		{
			in:       slices.Values([]int{1, 2, 3, 14, 15, 20, 31}),
			g:        func(i int) int { return i / 10 },
			expected: [][]int{{1, 2, 3}, {14, 15}, {20}, {31}},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			it := SeqBatchGroup(tt.in, tt.g)
			actual := slices.Collect(it)
			require.Equal(t, tt.expected, actual)
		})
	}

}
