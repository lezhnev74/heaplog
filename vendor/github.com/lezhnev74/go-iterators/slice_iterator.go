package go_iterators

// SliceIterator implements Iterator over a static slice
type SliceIterator[T any] struct {
	values   []T
	pos      int
	isClosed bool
}

func (s *SliceIterator[T]) Close() error {
	if s.isClosed {
		return ClosedIterator
	}
	s.isClosed = true
	return nil
}
func (s *SliceIterator[T]) Next() (v T, err error) {
	if s.pos >= len(s.values) {
		err = EmptyIterator
		return
	}
	v = s.values[s.pos]
	s.pos++
	return
}

func NewSliceIterator[T any](values []T) Iterator[T] {
	return &SliceIterator[T]{
		values: values,
		pos:    0,
	}
}
