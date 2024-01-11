# Go Iterators

[![Build](https://github.com/lezhnev74/go-iterators/actions/workflows/go.yml/badge.svg)](https://github.com/lezhnev74/go-iterators/actions/workflows/go.yml)
![Code Coverage](https://raw.githubusercontent.com/lezhnev74/go-iterators/badges/.badges/main/coverage.svg)

Since Go does not have a default iterator type (though there are
discussions [here](https://bitfieldconsulting.com/golang/iterators), [here](https://github.com/golang/go/issues/61897)
and [there](https://ewencp.org/blog/golang-iterators/)), here is a set of different iterators crafted manually.
Particularly, there is [a proposal](https://github.com/golang/go/issues/61898) for a package that defines compound
operations on iterators, like merging/selecting. Until Go has a stdlib's iterator implementation (or at least an
experimental standalone package), there is this package.

## Iterator Interface

```go
// Iterator is used for working with sequences of possibly unknown size
// Interface adds a performance penalty for indirection.
type Iterator[T any] interface {
  // Next returns EmptyIterator when no value available at the source
  // error == nil means the returned value is good
  Next() (T, error)
  // Closer the client may decide to stop the iteration before EmptyIterator received
  // After the first call it must return ClosedIterator.
  io.Closer
}
```

## Various Iterators

Single iterators
- `CallbackIterator` calls a function to provide the next value
- `SliceIterator` iterates over a static precalculated slice
- `DynamicSliceIterator` behaves like `SliceIterator` but fetches a new slice when previous slice is "empty"

Compound iterators

Unary:
- `ClosingIterator` adds custom Closing logic on top of another iterator
- `BatchingIterator` buffers internal iterator and returns slices of values
- `FilteringIterator` filters values from internal iterator 
- `MappingIterator` maps values from the inner iterator

Binary:
- `SortedSelectingIterator` combines 2 sorted iterators into a single sorted iterator.
- `UniqueSelectingIterator` The same as `SelectingIterator` but removes duplicates.
- `DiffIterator` returns all from the first iterator that is not present in the second

## Design notes

- compound iterators proxy errors from internal iterators
- compound iterators close internal iterators upon emptying
- compound binary iterators enable making efficient selection trees
