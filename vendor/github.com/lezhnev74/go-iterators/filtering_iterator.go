package go_iterators

// FilteringIterator filters values from internal iterator
type FilteringIterator[T any] struct {
	innerIterator Iterator[T]
	filter        func(T) bool
}

func (f FilteringIterator[T]) Next() (T, error) {
	for {
		v, err := f.innerIterator.Next()
		if err != nil || f.filter(v) {
			return v, err
		}
	}
}

func (f FilteringIterator[T]) Close() error {
	return f.innerIterator.Close()
}

func NewFilteringIterator[T any](inner Iterator[T], filter func(T) bool) Iterator[T] {
	return &FilteringIterator[T]{
		innerIterator: inner,
		filter:        filter,
	}
}
