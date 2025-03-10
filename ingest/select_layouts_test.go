package ingest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog_2024/common"
	"heaplog_2024/scanner"
	"testing"
)

func TestSelectLayouts(t *testing.T) {
	layouts := []scanner.MessageLayout{
		{From: 10, To: 20},
		{From: 20, To: 30},
		{From: 30, To: 40},
	}

	type test struct {
		loc             common.Location
		expectedLayouts []scanner.MessageLayout
	}

	tests := []test{
		{ // includes all
			loc:             common.Location{From: 0, To: 100},
			expectedLayouts: layouts,
		},
		{ // includes nothing left
			loc:             common.Location{From: 0, To: 9},
			expectedLayouts: []scanner.MessageLayout{},
		},
		{ // includes nothing right
			loc:             common.Location{From: 1000, To: 2000},
			expectedLayouts: []scanner.MessageLayout{},
		},
		{ // smaller than a message
			loc: common.Location{From: 10, To: 12},
			expectedLayouts: []scanner.MessageLayout{
				{From: 10, To: 20},
			},
		},
		{ // no message starts in the location
			loc:             common.Location{From: 11, To: 12},
			expectedLayouts: []scanner.MessageLayout{},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			actualLayouts := selectLocationLayouts(tt.loc, layouts)
			require.Equal(t, tt.expectedLayouts, actualLayouts)
		})
	}
}
