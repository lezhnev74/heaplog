package go_iterators

// SortedSelectingIterator returns sorted values from two other iterators
// if iterators are not sorted the behaviour is less predictable.
type SortedSelectingIterator[T any] struct {
	SelectingIterator[T]
}

func (si *SortedSelectingIterator[T]) Next() (v T, err error) {
	err = si.fetch()
	if err != nil {
		return
	}

	if !si.v1Fetched && !si.v2Fetched {
		err = EmptyIterator
		return
	}

	// 1. only v1
	if si.v1Fetched && !si.v2Fetched {
		si.v1Fetched = false
		v = si.v1
		return
	}
	// 2. only v2
	if si.v2Fetched && !si.v1Fetched {
		si.v2Fetched = false
		v = si.v2
		return
	}
	// 3. both present
	r := si.cmp(si.v1, si.v2)
	if r == 0 {
		si.v1Fetched = false
		v = si.v1
		return
	} else if r < 0 {
		si.v1Fetched = false
		v = si.v1
		return
	} else {
		si.v2Fetched = false
		v = si.v2
		return
	}
}

func NewSortedSelectingIterator[T any](it1, it2 Iterator[T], cf CmpFunc[T]) Iterator[T] {
	return &SortedSelectingIterator[T]{
		SelectingIterator[T]{
			it1: it1,
			it2: it2,
			cmp: cf,
		},
	}
}
