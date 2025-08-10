package common

// ErrVal represents a container for a value of type V and an associated error.
// Type parameter V can be any type.
type ErrVal[V any] struct {
	Err error // The error that occurred during value processing, if any
	Val V     // The value being wrapped
}

func NewErrValE[V any](err error) ErrVal[V] {
	return ErrVal[V]{
		Err: err,
	}
}

func NewErrValV[V any](val V) ErrVal[V] {
	return ErrVal[V]{
		Val: val,
	}
}
