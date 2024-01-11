package go_iterators

// DiffIterator returns all from it1 that are not present in it2
type DiffIterator[T any] struct {
	SelectingIterator[T]
}

func (si *DiffIterator[T]) Next() (v T, err error) {
	// check if both values present and collapse, fetch more if so
	err = si.fetch()
	for {
		if err != nil {
			return
		}
		if si.v1Fetched && si.v2Fetched && si.cmp(si.v1, si.v2) == 0 {
			si.v1Fetched, si.v2Fetched = false, false
			err = si.fetch()
			continue
		}
		break
	}

	if si.v1Fetched {
		si.v1Fetched = false
		v = si.v1
		return
	}

	err = EmptyIterator
	return
}

func NewRemovingIterator[T any](itMain, itRemove Iterator[T], cf CmpFunc[T]) Iterator[T] {
	return &DiffIterator[T]{
		SelectingIterator[T]{
			it1: itMain,
			it2: itRemove,
			cmp: cf,
		},
	}
}
