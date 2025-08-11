package ingest

import (
	"cmp"
	"slices"

	"heaplog_2024/internal/common"
)

// alignByLayouts groups layouts into contiguous segments based on the given segmentSize.
// Note: The final segment may be smaller than segmentSize if there are not enough layouts
// to fill it.
// Note: both locs and layouts must be clustered by their From field.
func alignByLayouts(
	segmentSize int,
	locs []common.Location,
	layouts []common.Location,
) [][]common.Location {
	result := make([][]common.Location, 0)
	if len(layouts) == 0 || len(locs) == 0 {
		return result
	}

	currentSegment := make([]common.Location, 0)
	currentSize := 0
	latestLayoutIndex := 0

	for _, loc := range locs {
		li, found := slices.BinarySearchFunc(
			layouts[latestLayoutIndex:],
			loc,
			func(loc common.Location, layout common.Location) int { return cmp.Compare(loc.From, layout.From) },
		)
		li += latestLayoutIndex // correct the index
		if !found && li == len(layouts) {
			continue
		}

		latestLayoutIndex = li
		for latestLayoutIndex < len(layouts) && loc.Intersects(layouts[latestLayoutIndex]) {
			layout := layouts[latestLayoutIndex]
			// Check if layout abuts with previous layout in segment
			if len(currentSegment) > 0 && currentSegment[len(currentSegment)-1].To != layout.From {
				// Start new segment if layouts don't abut
				if len(currentSegment) > 0 {
					result = append(result, currentSegment)
				}
				currentSegment = make([]common.Location, 0)
				currentSize = 0
			}

			// Add to current segment
			currentSegment = append(currentSegment, layout)
			currentSize += layout.Len()

			// Segment full → flush
			if currentSize >= segmentSize {
				result = append(result, currentSegment)
				currentSegment = make([]common.Location, 0)
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
