package common

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestGroupSlice(t *testing.T) {
	type test struct {
		in       []string
		g        func(string) string
		expected [][]string
	}

	var empty [][]string
	tests := []test{
		{
			in:       []string{},
			g:        func(i string) string { return strconv.Itoa(len(i)) },
			expected: empty,
		},
		{
			in:       []string{"a"},
			g:        func(i string) string { return strconv.Itoa(len(i)) },
			expected: [][]string{{"a"}},
		},
		{
			in:       []string{"a", "b", "aa", "bcd"},
			g:        func(i string) string { return strconv.Itoa(len(i)) },
			expected: [][]string{{"a", "b"}, {"aa"}, {"bcd"}},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			out := GroupSlice(tt.in, tt.g)
			require.Equal(t, tt.expected, out)
		})
	}
}

func TestSliceAny(t *testing.T) {
	type test struct {
		in          []int
		expectedOut []any
	}

	tests := []test{
		{
			[]int{},
			[]any{},
		},
		{
			[]int{1},
			[]any{1},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("inputQuery %v", tt.in), func(t *testing.T) {
			out := SliceToAny(tt.in)
			require.Equal(t, tt.expectedOut, out)
		})
	}
}
