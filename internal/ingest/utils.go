package ingest

import (
	"cmp"
	"iter"
	"slices"
	"unsafe"

	"heaplog_2024/internal/common"
)

// alignSegmentsByMessageBoundaries groups message layouts into segments within locations.
// It ensures that layouts within each layouts are contiguous (abutting) and the total size.
func alignSegmentsByMessageBoundaries(
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

// findMisalignedSegments checks misalignment of indexed segments with actual message boundaries found in the file.
func findMisalignedSegments(
	indexedSegments map[string][]common.Location,
	foundFilesLayouts map[string][]MessageLayout,
) iter.Seq[string] {
	return func(yield func(string) bool) {
	indexFileLoop:
		for file, indexedLocs := range indexedSegments {
			if len(indexedLocs) == 0 {
				continue // no point in comparing, no indexed data available
			}

			// map indexedLocs to actual messages in the file
			for _, s := range indexedLocs {
				leftMatched, rightMatched := false, false
				for _, m := range foundFilesLayouts[file] {
					if s.From == m.Loc.From {
						leftMatched = true
					}
					if s.To == m.Loc.To {
						rightMatched = true
					}
				}
				if leftMatched && rightMatched {
					continue
				}

				if !yield(file) {
					continue indexFileLoop
				}
			}
		}
	}
}

// filesWithIncompleteTrailingSegments identifies files where the last indexed segment is incomplete
// (smaller than segmentLen) but doesn't reach the end of the file, indicating potential unindexed data.
func filesWithIncompleteTrailingSegments(
	segmentLen int,
	indexedSegments map[string][]common.Location,
	accessibleFiles map[string]int,
) iter.Seq[string] {
	return func(yield func(string) bool) {
		for file, segments := range indexedSegments {
			if len(segments) == 0 {
				continue
			}

			lastSegment := segments[len(segments)-1]
			size := accessibleFiles[file]

			if lastSegment.Len() < segmentLen && lastSegment.To < size {
				if !yield(file) {
					return
				}
			}
		}
	}
}
