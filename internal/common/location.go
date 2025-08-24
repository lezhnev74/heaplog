package common

import (
	"fmt"
	"slices"
)

// Location represents a contiguous range of bytes: [From,To)
type Location struct {
	From, To int
}

func NewLocation(from, to int) Location {
	l := Location{from, to}
	if l.Len() < 0 {
		panic(fmt.Sprintf("negative location length: [%d,%d)", l.From, l.To))
	}
	return l
}

func (s Location) Len() int { return s.To - s.From }

func (s Location) Contains(i int) bool { return i >= s.From && i < s.To }

func (s Location) Intersects(s2 Location) bool { return s.From <= s2.To && s.To >= s2.From }

// Split slices a segment into chunks of at most maxLen bytes
func (s Location) Split(maxLen int) (ret []Location) {
	for {
		if int(s.Len()) <= maxLen {
			ret = append(ret, s)
			return
		}

		ret = append(ret, Location{s.From, s.From + maxLen})
		s = Location{s.From + maxLen, s.To}
	}
}

// Remove returns a list of locations that remain after removing s2 from s.
// If there is no intersection, returns the original location.
// If there is partial overlap, returns one or two locations that remain.
// If s2 fully contains s, returns the empty list.
func (s Location) Remove(s2 Location) (ret []Location) {
	intersection := Location{max(s.From, s2.From), min(s.To, s2.To)}

	// If the intersection is empty, then the difference is the union of the two ranges.
	if intersection.Len() < 0 {
		return []Location{s}
	}

	// Otherwise, the difference is the two ranges minus the intersection.
	result := Location{From: s.From, To: intersection.From}
	if result.Len() > 0 {
		ret = append(ret, result)
	}
	result = Location{From: intersection.To, To: s.To}
	if result.Len() > 0 {
		ret = append(ret, result)
	}
	return
}

// RemoveAll removes all locations from the original location.
func (s Location) RemoveAll(ss []Location) (ret []Location) {
	ret = []Location{s}
	for _, s2 := range ss {
		next := make([]Location, 0)
		for _, r := range ret {
			next = append(next, r.Remove(s2)...)
		}
		ret = next
	}
	return
}

// MergeLocations combines overlapping locations into continuous ranges.
// It sorts the input slice by From field and merges intersecting locations.
// Returns a new slice of merged locations in ascending order.
func MergeLocations(src []Location) (ret []Location) {
	slices.SortFunc(src, func(a, b Location) int { return int(a.From - b.From) })

	if len(src) < 2 {
		return src
	}

	cur := src[0]

	for i := 1; i < len(src); i++ {
		if src[i].Intersects(cur) {
			cur = Location{min(cur.From, src[i].From), max(cur.To, src[i].To)}
			continue
		}

		ret = append(ret, cur)
		cur = src[i]
	}

	ret = append(ret, cur)

	return
}

// ExcludeLocations removes exclusion ranges from source Location and returns remaining non-overlapping Locations
func ExcludeLocations(src Location, excl ...Location) []Location {
	cleanLocations := []Location{src}
	for _, el := range excl {
		nextPending := make([]Location, 0, len(cleanLocations))
		for _, pendingLocation := range cleanLocations {
			nextPending = append(nextPending, pendingLocation.Remove(el)...)
		}
		cleanLocations = nextPending
	}
	// Merge siblings to make contiguous locations
	return MergeLocations(cleanLocations)
}

// PickNextLocation returns a new Location within locations starting at minPos with length up to maxLen.
// Returns zero Location if no suitable location is found.
func PickNextLocation(locations []Location, minPos, maxLen int) Location {
	for _, l := range locations {
		if l.Contains(minPos) {
			return Location{From: minPos, To: min(minPos+maxLen, l.To)}
		}
	}
	return Location{}
}
