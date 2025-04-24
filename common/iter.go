package common

import "iter"

// SeqBatchGroup scans the incoming iterator and groups items via g()
func SeqBatchGroup[V any, C comparable](s iter.Seq[V], g func(V) C) iter.Seq[[]V] {
	return func(yield func([]V) bool) {
		var (
			lastG C
			batch []V
		)
		for v := range s {
			vg := g(v)
			if vg != lastG {
				lastG = vg
				if batch != nil && !yield(batch) {
					return
				}
				batch = nil
			}

			batch = append(batch, v)
		}

		if batch != nil {
			yield(batch)
		}
	}
}

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

// NopSeq is a convenience function
// which returns an empty no-op Seq
// that is safe to range over.
func NopSeq[T any]() iter.Seq[T] {
	return func(func(T) bool) {}
}

// NopSeq2 is the same as NopSeq, but for Seq2.
func NopSeq2[K, V any]() iter.Seq2[K, V] {
	return func(func(K, V) bool) {}
}
