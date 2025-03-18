package common

import "iter"

func SeqBatch[V any](s iter.Seq[V], size int) iter.Seq[[]V] {
	return func(yield func([]V) bool) {
		batch := make([]V, 0, size)
		for v := range s {
			batch = append(batch, v)
			if len(batch) == size {
				if !yield(batch) {
					return
				}
				batch = nil
			}
		}

		if len(batch) > 0 {
			yield(batch)
		}
	}
}
