package go_iterators

import (
	"errors"
	"fmt"
)

// BatchingIterator buffers internal iterator and returns slices of values
type BatchingIterator[T any] struct {
	innerIterator Iterator[T]
	batchSize     int
}

func NewBatchingIterator[T any](inner Iterator[T], batchSize int) Iterator[[]T] {
	if batchSize < 1 {
		panic(fmt.Sprintf("batch size is too low: %d", batchSize))
	}
	return &BatchingIterator[T]{inner, batchSize}
}

func (b *BatchingIterator[T]) Next() (v []T, err error) {

	v = make([]T, 0, b.batchSize)
	var item T

	for {
		item, err = b.innerIterator.Next()

		if err != nil {
			break
		}

		v = append(v, item)

		if len(v) == b.batchSize {
			break
		}
	}

	if errors.Is(err, EmptyIterator) && len(v) > 0 {
		err = nil
	}

	return
}

func (b *BatchingIterator[T]) Close() error {
	return b.innerIterator.Close()
}
