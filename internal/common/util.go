package common

// ChunksN splits items into n contiguous chunks with sizes as even as possible.
// The first (len(items) % chunks) chunks get one extra element.
func ChunksN[T any](items []T, n int) [][]T {
	if n <= 0 {
		return nil
	}
	out := make([][]T, 0, n)
	L := len(items)
	base := L / n
	rem := L % n

	start := 0
	for i := 0; i < n; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		if end > L {
			end = L
		}
		out = append(out, items[start:end])
		start = end
	}
	return out
}
