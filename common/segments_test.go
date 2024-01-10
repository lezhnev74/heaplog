package common

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIntersect(t *testing.T) {
	tests := []struct {
		a, b     Location
		expected bool
	}{
		{Location{0, 1}, Location{2, 3}, false},
		{Location{2, 3}, Location{0, 1}, false},
		{Location{0, 1}, Location{1, 2}, true},
		{Location{1, 2}, Location{0, 1}, true},
		{Location{1, 2}, Location{1, 2}, true},
		{Location{1, 3}, Location{2, 2}, true},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actual := tt.a.Intersects(tt.b)
			require.EqualValues(t, tt.expected, actual)
		})
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		src      []Location
		expected []Location
	}{
		{
			[]Location{{0, 1}},
			[]Location{{0, 1}},
		},
		{
			[]Location{{0, 1}, {2, 3}},
			[]Location{{0, 1}, {2, 3}},
		},
		{
			[]Location{{0, 1}, {1, 2}},
			[]Location{{0, 2}},
		},
		{
			[]Location{{0, 1}, {0, 2}},
			[]Location{{0, 2}},
		},
		{
			[]Location{{0, 3}, {2, 4}},
			[]Location{{0, 4}},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actual := MergeSegmentLocations(tt.src)
			require.EqualValues(t, tt.expected, actual)
		})
	}
}

func TestSingleSplit(t *testing.T) {
	tests := []struct {
		a    Location
		size int64
		r    []Location
	}{
		{
			// Split in equal max sizes
			Location{0, 3},
			2,
			[]Location{{0, 2}, {2, 3}},
		},
		{
			// 1 full segment
			Location{0, 1},
			2,
			[]Location{{0, 1}},
		},
		{
			// 1 partial segment
			Location{0, 1},
			10,
			[]Location{{0, 1}},
		},
		{
			// 2 partial segment
			Location{0, 3},
			3,
			[]Location{{0, 3}},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			r := tt.a.Split(tt.size)
			require.EqualValues(t, tt.r, r)
		})
	}
}

func TestSingleRemove(t *testing.T) {
	var empty []Location
	tests := []struct {
		a, b Location
		c    []Location
	}{
		{
			//  non overlap:
			//  a +-------+
			//	b  				+------+
			Location{0, 10},
			Location{20, 30},
			[]Location{{0, 10}},
		},
		{
			// non overlap:
			// a               +-------+
			// b +------+
			Location{10, 20},
			Location{0, 2},
			[]Location{{10, 20}},
		},
		{
			// equal:
			// a +---+
			// b +---+
			Location{0, 3},
			Location{0, 3},
			empty,
		},
		{
			// containing:
			// a   +---+
			// b +--------+
			Location{5, 6},
			Location{0, 10},
			empty,
		},
		{
			// equal:
			// a +
			// b +
			Location{0, 0},
			Location{0, 0},
			empty,
		},
		{
			// partial:
			// a +---+
			// b  	+---+
			Location{0, 3},
			Location{3, 6},
			[]Location{{0, 3}},
		},
		{
			// partial:
			// a     +---+
			// b +---+
			Location{5, 8},
			Location{3, 5},
			[]Location{{5, 8}},
		},
		{
			// partial:
			// a +-------+
			// b  	  +------+
			Location{0, 10},
			Location{5, 20},
			[]Location{{0, 5}},
		},
		{
			// partial:
			// a +-------+
			// b  	  ++
			Location{0, 10},
			Location{8, 9},
			[]Location{{0, 8}, {9, 10}},
		},
		{
			// partial:
			// a     +-------+
			// b +------+
			Location{10, 20},
			Location{5, 15},
			[]Location{{15, 20}},
		},
		{
			// partial including:
			// a +---------+
			// b    +---+
			Location{0, 20},
			Location{5, 15},
			[]Location{{0, 5}, {15, 20}},
		},
		{
			// partial including (almost full):
			// a +--------+
			// b  +------+
			Location{1, 4},
			Location{2, 3},
			[]Location{{1, 2}, {3, 4}},
		},
		{
			// partial including (common left part):
			// a +--------+
			// b +---+
			Location{0, 10},
			Location{0, 5},
			[]Location{{5, 10}},
		},
		{
			// partial including (common right part):
			// a +--------+
			// b      +---+
			Location{0, 10},
			Location{6, 10},
			[]Location{{0, 6}},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			r := tt.a.Remove(tt.b)
			require.EqualValues(t, tt.c, r)
		})
	}
}

//
// func TestRemove(t *testing.T) {
//	tests := []struct {
//		src            Location
//		others         []Location
//		expectedResult []Location
//	}{
//		{
//			Location{0, 100},
//			[]Location{},
//			[]Location{{0, 100}},
//		},
//		{
//			//		   +--+
//			// 		+-------+
//			Location{0, 100},
//			[]Location{{10, 20}},
//			[]Location{{0, 9}, {21, 100}},
//		},
//	}
//
//	for i, tt := range tests {
//		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
//			r := tt.src.removeSegments(tt.others)
//			require.EqualValues(t, tt.expectedResult, r)
//		})
//	}
// }
