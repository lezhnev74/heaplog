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
		{ // no intersection on both ends
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
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := ExcludeLocations(tt.src, tt.excl...); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ExcludeLocations() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestLocation_Intersects(t *testing.T) {
	tests := []struct {
		loc1 Location
		loc2 Location
		want bool
	}{
		{ // exact match
			Location{0, 100},
			Location{0, 100},
			true,
		},
		{ // complete overlap
			Location{0, 100},
			Location{25, 75},
			true,
		},
		{ // partial overlap at the start
			Location{0, 50},
			Location{25, 75},
			true,
		},
		{ // partial overlap at the end
			Location{25, 75},
			Location{50, 100},
			true,
		},
		{ // no intersection before
			Location{0, 25},
			Location{50, 75},
			false,
		},
		{ // no intersection after
			Location{75, 100},
			Location{0, 25},
			false,
		},
		{ // touching at the end point
			Location{0, 50},
			Location{50, 100},
			true,
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := tt.loc1.Intersects(tt.loc2); got != tt.want {
					t.Errorf("Location.Intersects() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestLocation_Split(t *testing.T) {
	tests := []struct {
		loc    Location
		maxLen int
		want   []Location
	}{
		{ // exact size
			Location{0, 100},
			100,
			[]Location{{0, 100}},
		},
		{ // split into two
			Location{0, 100},
			50,
			[]Location{{0, 50}, {50, 100}},
		},
		{ // split into three
			Location{0, 100},
			40,
			[]Location{{0, 40}, {40, 80}, {80, 100}},
		},
		{ // smaller than maxLen
			Location{0, 50},
			100,
			[]Location{{0, 50}},
		},
		{ // uneven split
			Location{0, 95},
			30,
			[]Location{{0, 30}, {30, 60}, {60, 90}, {90, 95}},
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := tt.loc.Split(tt.maxLen); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Location.Split() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestLocation_Contains(t *testing.T) {
	tests := []struct {
		loc  Location
		pos  int
		want bool
	}{
		{ // exact match at the start
			Location{0, 100},
			0,
			true,
		},
		{ // exact match at the end-1
			Location{0, 100},
			99,
			true,
		},
		{ // exact match at the end (should be false as the end is exclusive)
			Location{0, 100},
			100,
			false,
		},
		{ // middle of th range
			Location{50, 150},
			75,
			true,
		},
		{ // before the range
			Location{50, 150},
			25,
			false,
		},
		{ // after the range
			Location{50, 150},
			175,
			false,
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := tt.loc.Contains(tt.pos); got != tt.want {
					t.Errorf("Location.Contains() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestLocation_Remove(t *testing.T) {
	tests := []struct {
		loc1 Location
		loc2 Location
		want []Location
	}{
		{ // exact match - no remainder
			Location{0, 100},
			Location{0, 100},
			[]Location(nil),
		},
		{ // no intersection - original location returned
			Location{0, 50},
			Location{75, 100},
			[]Location{{0, 50}},
		},
		{ // remove from the middle - two locations remain
			Location{0, 100},
			Location{40, 60},
			[]Location{{0, 40}, {60, 100}},
		},
		{ // remove from start - one location remains
			Location{0, 100},
			Location{0, 50},
			[]Location{{50, 100}},
		},
		{ // remove from end - one location remains
			Location{0, 100},
			Location{75, 100},
			[]Location{{0, 75}},
		},
		{ // partial overlap at the start
			Location{50, 150},
			Location{0, 100},
			[]Location{{100, 150}},
		},
		{ // partial overlap at the end
			Location{0, 100},
			Location{50, 150},
			[]Location{{0, 50}},
		},
		{ // touching at the end point
			Location{0, 100},
			Location{100, 150},
			[]Location{{0, 100}},
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := tt.loc1.Remove(tt.loc2); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Location.Remove() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestMergeLocations(t *testing.T) {
	tests := []struct {
		locations []Location
		want      []Location
	}{
		{ // empty input
			[]Location{},
			[]Location{},
		},
		{ // single location
			[]Location{{0, 100}},
			[]Location{{0, 100}},
		},
		{ // non-overlapping locations
			[]Location{{0, 50}, {75, 100}},
			[]Location{{0, 50}, {75, 100}},
		},
		{ // overlapping locations
			[]Location{{0, 75}, {50, 100}},
			[]Location{{0, 100}},
		},
		{ // multiple overlapping locations
			[]Location{{0, 50}, {25, 75}, {60, 100}},
			[]Location{{0, 100}},
		},
		{ // touching locations
			[]Location{{0, 50}, {50, 100}},
			[]Location{{0, 100}},
		},
		{ // unsorted input
			[]Location{{75, 100}, {0, 25}, {20, 50}},
			[]Location{{0, 50}, {75, 100}},
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := MergeLocations(tt.locations); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("MergeLocations() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestExcludeLocations1(t *testing.T) {
	tests := []struct {
		src  Location
		excl []Location
		want []Location
	}{
		{ // nested exclusion
			Location{0, 100},
			[]Location{{25, 75}},
			[]Location{{0, 25}, {75, 100}},
		},
		{ // multiple non-overlapping exclusions
			Location{0, 100},
			[]Location{{10, 20}, {30, 40}, {50, 60}},
			[]Location{{0, 10}, {20, 30}, {40, 50}, {60, 100}},
		},
		{ // overlapping exclusions
			Location{0, 100},
			[]Location{{10, 30}, {20, 40}},
			[]Location{{0, 10}, {40, 100}},
		},
		{ // exclusion at the start
			Location{0, 100},
			[]Location{{0, 25}},
			[]Location{{25, 100}},
		},
		{ // exclusion at the end
			Location{0, 100},
			[]Location{{75, 100}},
			[]Location{{0, 75}},
		},
		{ // non overlapping at all
			Location{0, 100},
			[]Location{{200, 300}},
			[]Location{{0, 100}},
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := ExcludeLocations(tt.src, tt.excl...); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ExcludeLocations() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestPickNextLocation(t *testing.T) {
	tests := []struct {
		locations []Location
		minPos    int
		maxLen    int
		want      Location
	}{
		{ // exact match
			[]Location{{0, 100}},
			0,
			100,
			Location{0, 100},
		},
		{ // partial match with maxLen limit
			[]Location{{0, 100}},
			50,
			30,
			Location{50, 80},
		},
		{ // position in middle of multiple locations
			[]Location{{0, 50}, {75, 100}},
			25,
			40,
			Location{25, 50},
		},
		{ // position in middle of multiple locations where later has a bigger length
			[]Location{{0, 50}, {75, 200}},
			25,
			40,
			Location{25, 50},
		},
		{ // position outside any location
			[]Location{{0, 50}, {75, 100}},
			100,
			20,
			Location{0, 0},
		},
		{ // position at the end of location
			[]Location{{0, 50}, {75, 100}},
			75,
			50,
			Location{75, 100},
		},
		{ // empty locations
			[]Location{},
			0,
			100,
			Location{0, 0},
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := PickNextLocation(tt.locations, tt.minPos, tt.maxLen); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("PickNextLocation() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestNewLocation(t *testing.T) {
	tests := []struct {
		from    int
		to      int
		want    Location
		wantErr bool
	}{
		{ // valid location
			0,
			100,
			Location{0, 100},
			false,
		},
		{ // zero length location
			50,
			50,
			Location{50, 50},
			false,
		},
		{ // single unit location
			75,
			76,
			Location{75, 76},
			false,
		},
		{ // invalid location (negative length)
			100,
			99,
			Location{},
			true,
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if tt.wantErr {
					defer func() {
						if r := recover(); r == nil {
							t.Errorf("NewLocation() should have panicked")
						}
					}()
				}
				if got := NewLocation(tt.from, tt.to); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("NewLocation() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func TestLocation_RemoveAll(t *testing.T) {
	tests := []struct {
		loc  Location
		excl []Location
		want []Location
	}{
		{ // empty exclusion list
			Location{0, 100},
			[]Location{},
			[]Location{{0, 100}},
		},
		{ // single exclusion in the middle
			Location{0, 100},
			[]Location{{40, 60}},
			[]Location{{0, 40}, {60, 100}},
		},
		{ // multiple non-overlapping exclusions
			Location{0, 100},
			[]Location{{10, 20}, {40, 50}, {70, 80}},
			[]Location{{0, 10}, {20, 40}, {50, 70}, {80, 100}},
		},
		{ // overlapping exclusions
			Location{0, 100},
			[]Location{{10, 30}, {20, 40}, {35, 45}},
			[]Location{{0, 10}, {45, 100}},
		},
		{ // complete coverage
			Location{0, 100},
			[]Location{{0, 50}, {50, 100}},
			[]Location{},
		},
		{ // no intersection
			Location{0, 100},
			[]Location{{150, 200}, {250, 300}},
			[]Location{{0, 100}},
		},
	}
	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				if got := tt.loc.RemoveAll(tt.excl); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Location.RemoveAll() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}
