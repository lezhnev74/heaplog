package go_iterators

import "errors"

// ClosingIterator adds custom Closing logic on top of another iterator
type ClosingIterator[T any] struct {
	innerIterator Iterator[T]
	// extra function called AFTER "innerErr := Close()" returns
	close    func(innerErr error) error
	isClosed bool
}

func (c *ClosingIterator[T]) Next() (T, error) {
	return c.innerIterator.Next()
}

func (c *ClosingIterator[T]) Close() error {
	if c.isClosed {
		return ClosedIterator
	}
	err := c.innerIterator.Close()
	err = c.close(err)

	// Close it if no errors happened or if the inner iterator has been closed already
	if err == nil || errors.Is(err, ClosedIterator) {
		c.isClosed = true
		err = nil
	}

	return err
}

func NewClosingIterator[T any](innerIterator Iterator[T], close func(innerErr error) error) Iterator[T] {
	return &ClosingIterator[T]{
		innerIterator: innerIterator,
		close:         close,
	}
}
