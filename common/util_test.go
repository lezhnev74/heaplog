package common

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"path"
	"testing"
)

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

func TestFilterSliceInPlace(t *testing.T) {
	type test struct {
		in       []int
		expected []int
		filter   func(i int) bool
	}
	tests := []test{
		{[]int{0, 2, 4}, []int{}, func(i int) bool { return i%2 != 0 }},
		{[]int{1, 2, 3, 4}, []int{1, 3}, func(i int) bool { return i%2 != 0 }},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.in), func(t *testing.T) {
			actual := FilterSliceInPlace(tt.in, tt.filter)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestGetFilenames(t *testing.T) {
	root, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	files := []string{
		path.Join(root, "file1"),
		path.Join(root, "file2"),
	}

	for _, f := range files {
		_ = os.WriteFile(f, []byte("hello"), os.ModePerm)
	}

	readFiles, _ := GetFilenames(root)
	require.EqualValues(t, files, readFiles)

}

func TestCompressUint64(t *testing.T) {
	numbers := []uint64{0, 500, math.MaxUint64}

	compressed, err := CompressUint64(numbers)
	require.NoError(t, err)

	decompressedNumbers, err := DecompressUint64(compressed)
	require.NoError(t, err)

	require.EqualValues(t, numbers, decompressedNumbers)
}
