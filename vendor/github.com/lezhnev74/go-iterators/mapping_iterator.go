package go_iterators

// MappingIterator maps values from the inner iterator
type MappingIterator[T any, InnerT any] struct {
	innerIterator Iterator[InnerT]
	mf            func(InnerT) T
}

func (m *MappingIterator[T, InnerT]) Next() (T, error) {
	v, err := m.innerIterator.Next()
	return m.mf(v), err
}

func (m *MappingIterator[T, InnerT]) Close() error {
	return m.innerIterator.Close()
}

func NewMappingIterator[T any, InnerT any](inner Iterator[InnerT], mf func(InnerT) T) Iterator[T] {
	return &MappingIterator[T, InnerT]{
		innerIterator: inner,
		mf:            mf,
	}
}
