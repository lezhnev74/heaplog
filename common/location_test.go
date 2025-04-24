package common

import (
	"fmt"
	"reflect"
	"testing"
)

func TestExcludeLocations(t *testing.T) {
	tests := []struct {
		src  Location
		excl []Location
		want []Location
	}{
		{ // exact
			Location{0, 100},
			[]Location{{0, 100}},
			[]Location{},
		},
		{ // covers all
			Location{10, 20},
			[]Location{{0, 100}},
			[]Location{},
		},
		{ // non intersect on both ends
			Location{10, 20},
			[]Location{{0, 10}, {20, 30}},
			[]Location{{10, 20}},
		},
		{
			Location{10, 20},
			[]Location{{0, 11}},
			[]Location{{11, 20}},
		},
		{
			Location{10, 20},
			[]Location{{0, 19}},
			[]Location{{19, 20}},
		},
		{
			Location{10, 20},
			[]Location{{0, 15}, {17, 18}},
			[]Location{{15, 17}, {18, 20}},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			if got := ExcludeLocations(tt.src, tt.excl...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExcludeLocations() = %v, want %v", got, tt.want)
			}
		})
	}
}
