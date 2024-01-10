package common

import (
	"log"
	"slices"
)

// Location addresses an area of bytes [Min,Max)
type Location struct {
	Min, Max int64
}

func (s Location) Intersects(s2 Location) bool {
	return s.Min <= s2.Max && s.Max >= s2.Min
}

// Split slices a segment into many
func (s Location) Split(maxLen int64) (ret []Location) {

	for {
		if s.len() <= maxLen {
			ret = append(ret, s)
			return
		}

		ret = append(ret, Location{s.Min, s.Min + maxLen})
		s = Location{s.Min + maxLen, s.Max}
	}
}

func (s Location) len() int64 { return s.Max - s.Min }

func (s Location) Remove(s2 Location) (ret []Location) {

	// valid locations
	if s.len() < 0 || s2.len() < 0 {
		log.Panicf("Invalid ranges: %v or %v", s, s2)
	}

	intersection := Location{max(s.Min, s2.Min), min(s.Max, s2.Max)}

	// If the intersection is empty, then the difference is the union of the two ranges.
	if intersection.len() < 0 {
		return []Location{s}
	}

	// Otherwise, the difference is the two ranges minus the intersection.
	result := Location{Min: s.Min, Max: intersection.Min}
	if result.len() > 0 {
		ret = append(ret, result)
	}
	result = Location{Min: intersection.Max, Max: s.Max}
	if result.len() > 0 {
		ret = append(ret, result)
	}
	return
}

func MergeSegmentLocations(src []Location) (ret []Location) {
	slices.SortFunc(src, func(a, b Location) int { return int(a.Min - b.Min) })

	if len(src) < 2 {
		return src
	}

	cur := src[0]

	for i := 1; i < len(src); i++ {
		if src[i].Intersects(cur) {
			cur = Location{min(cur.Min, src[i].Min), max(cur.Max, src[i].Max)}
			continue
		}

		ret = append(ret, cur)
		cur = src[i]
	}

	ret = append(ret, cur)

	return
}
