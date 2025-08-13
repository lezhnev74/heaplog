package ingest

import (
	"cmp"
	"slices"
	"unsafe"

	"heaplog_2024/internal/common"
)

// segmentLayoutsByLocations groups message layouts into segments within locations.
// It ensures that layouts within each layouts are contiguous (abutting) and the total size.
func segmentLayoutsByLocations(
	segmentSize int,
	locs []common.Location,
	layouts []MessageLayout,
) [][]MessageLayout {
	result := make([][]MessageLayout, 0)
	if len(layouts) == 0 || len(locs) == 0 {
		return result
	}

	currentSegment := make([]MessageLayout, 0)
	currentSize := 0
	latestLayoutIndex := 0

	for _, loc := range locs {
		li, found := slices.BinarySearchFunc(
			layouts[latestLayoutIndex:],
			loc,
			func(a MessageLayout, b common.Location) int {
				if a.Loc.Intersects(b) {
					return 0
				}
				return cmp.Compare(a.Loc.From, b.From)
			},
		)
		li += latestLayoutIndex // correct the index
		if !found && li == len(layouts) {
			continue
		}

		latestLayoutIndex = li
		for latestLayoutIndex < len(layouts) && loc.Intersects(layouts[latestLayoutIndex].Loc) {
			layout := layouts[latestLayoutIndex]
			// Check if layout abuts with previous layout in layouts
			if len(currentSegment) > 0 && currentSegment[len(currentSegment)-1].Loc.To != layout.Loc.From {
				// Start new layouts if layouts don'tokenize abut
				if len(currentSegment) > 0 {
					result = append(result, currentSegment)
				}
				currentSegment = make([]MessageLayout, 0)
				currentSize = 0
			}

			// Add to current layouts
			currentSegment = append(currentSegment, layout)
			currentSize += layout.Loc.Len()

			// Segment full → flush
			if currentSize >= segmentSize {
				result = append(result, currentSegment)
				currentSegment = make([]MessageLayout, 0)
				currentSize = 0
			}

			latestLayoutIndex++
		}
	}

	// Add remaining layouts as final layouts
	if len(currentSegment) > 0 {
		result = append(result, currentSegment)
	}

	return result
}

// appendTermsUnique adds unique terms from the byte slices to the given map.
// It efficiently converts byte slices to strings using unsafe operations to avoid allocations.
// Parameters:
//   - all: destination map storing unique terms as keys
//   - terms: slice of byte slices containing terms to be added
func appendTermsUnique(all map[string]struct{}, terms [][]byte) {
	for _, t := range terms {
		s := unsafe.String(unsafe.SliceData(t), len(t))
		all[s] = struct{}{}
	}
}
