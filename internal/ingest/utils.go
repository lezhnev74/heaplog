package ingest

import (
	"cmp"
	"slices"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ingest/scanner"
)

// segmentLayoutsByLocations groups message layouts into segments within locations.
// It ensures that layouts within each segment are contiguous (abutting) and the total size.
func segmentLayoutsByLocations(
	segmentSize int,
	locs []common.Location,
	layouts []scanner.MessageLayout,
) [][]scanner.MessageLayout {
	result := make([][]scanner.MessageLayout, 0)
	if len(layouts) == 0 || len(locs) == 0 {
		return result
	}

	currentSegment := make([]scanner.MessageLayout, 0)
	currentSize := 0
	latestLayoutIndex := 0

	for _, loc := range locs {
		li, found := slices.BinarySearchFunc(
			layouts[latestLayoutIndex:],
			loc,
			func(a scanner.MessageLayout, b common.Location) int {
				if a.Intersects(b) {
					return 0
				}
				return cmp.Compare(a.From, b.From)
			},
		)
		li += latestLayoutIndex // correct the index
		if !found && li == len(layouts) {
			continue
		}

		latestLayoutIndex = li
		for latestLayoutIndex < len(layouts) && loc.Intersects(layouts[latestLayoutIndex].Location) {
			layout := layouts[latestLayoutIndex]
			// Check if layout abuts with previous layout in segment
			if len(currentSegment) > 0 && currentSegment[len(currentSegment)-1].To != layout.From {
				// Start new segment if layouts don't abut
				if len(currentSegment) > 0 {
					result = append(result, currentSegment)
				}
				currentSegment = make([]scanner.MessageLayout, 0)
				currentSize = 0
			}

			// Add to current segment
			currentSegment = append(currentSegment, layout)
			currentSize += layout.Len()

			// Segment full → flush
			if currentSize >= segmentSize {
				result = append(result, currentSegment)
				currentSegment = make([]scanner.MessageLayout, 0)
				currentSize = 0
			}

			latestLayoutIndex++
		}
	}

	// Add remaining layouts as final segment
	if len(currentSegment) > 0 {
		result = append(result, currentSegment)
	}

	return result
}
