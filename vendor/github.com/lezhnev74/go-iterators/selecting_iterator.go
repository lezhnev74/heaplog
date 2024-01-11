package go_iterators

import (
	"errors"
)

// SelectingIterator combines two iterators together.
// We can customize the selection logic with a custom "next" function.
// This iterator closes an internal iterator once it returns "EmptyIterator".
type SelectingIterator[T any] struct {
	it1, it2             Iterator[T]
	v1, v2               T    // prefetched from internal iterators
	v1Fetched, v2Fetched bool // is value prefetched
	cmp                  CmpFunc[T]
	next                 func() (v T, err error) // custom Next selector
	isClosed             bool
}

func (s *SelectingIterator[T]) Close() error {

	if s.isClosed {
		return ClosedIterator
	}
	s.isClosed = true

	if s.it1 != nil {
		err := s.it1.Close()
		s.it1 = nil
		if err != nil {
			s.it2.Close() // close anyway
			s.it2 = nil
			if !errors.Is(err, ClosedIterator) {
				return err
			}
		}
	}

	if s.it2 != nil {
		err := s.it2.Close()
		s.it2 = nil
		if err != nil && !errors.Is(err, ClosedIterator) {
			return err
		}
	}

	return nil
}

func (si *SelectingIterator[T]) fetch() error {
	var err error

	if si.it1 != nil && !si.v1Fetched {
		si.v1, err = si.it1.Next()
		si.v1Fetched = err == nil

		if errors.Is(err, EmptyIterator) {
			err = si.it1.Close()
			si.it1 = nil
		}
	}
	if err != nil && !errors.Is(err, EmptyIterator) {
		return err
	}

	if si.it2 != nil && !si.v2Fetched {
		si.v2, err = si.it2.Next()
		si.v2Fetched = err == nil

		if errors.Is(err, EmptyIterator) {
			err = si.it2.Close()
			si.it2 = nil
		}
	}
	if err != nil && !errors.Is(err, EmptyIterator) {
		return err
	}
	return nil
}
