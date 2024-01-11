package go_iterators

// DynamicSliceIterator implements Iterator over a dynamic slice
// whenever it needs data it calls fetch() for a new slice to iterate
type DynamicSliceIterator[T any] struct {
	values   []T
	fetch    func() ([]T, error) // nil or empty slice stops iteration
	close    func() error
	isClosed bool
}

func (s *DynamicSliceIterator[T]) Close() error {
	if s.isClosed {
		return ClosedIterator
	}
	s.isClosed = true
	return s.close()
}

// Next calls underlying fetch func,
// it is undefined if fetch function returns error AND value or error AND no value
func (s *DynamicSliceIterator[T]) Next() (v T, err error) {
	if len(s.values) == 0 {
		s.values, err = s.fetch()
		if err != nil {
			return
		}
	}

	if len(s.values) == 0 {
		err = EmptyIterator
		return
	}

	v = s.values[0]
	s.values = s.values[1:]
	return
}

func NewDynamicSliceIterator[T any](fetch func() ([]T, error), close func() error) Iterator[T] {
	return &DynamicSliceIterator[T]{
		fetch: fetch,
		close: close,
	}
}
