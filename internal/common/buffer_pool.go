// Package common provides shared utilities and data structures for the application.
package common

import (
	"sync"
)

// BufferPool maintains a pool of byte slices of predefined sizes
// to reduce memory allocations and GC pressure.
type BufferPool struct {
	pools map[int]*sync.Pool
	sizes []int
}

// NewBufferPool creates a new buffer pool with the given slice sizes.
// Each size will have its own sync.Pool instance.
func NewBufferPool(sizes []int) *BufferPool {
	pools := make(map[int]*sync.Pool, len(sizes))
	for _, sz := range sizes {
		pools[sz] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, sz)
			},
		}
	}
	return &BufferPool{pools: pools, sizes: sizes}
}

// Get returns a byte slice with capacity >= minSize.
// If no suitable buffer is found in the pool, a new one is allocated.
func (bp *BufferPool) Get(minSize int) []byte {
	for _, sz := range bp.sizes {
		if sz >= minSize {
			return bp.pools[sz].Get().([]byte)
		}
	}
	// fallback: exact size
	return make([]byte, minSize)
}

// Put returns a buffer to the pool if its capacity matches one of the predefined sizes.
// Buffers with non-matching sizes are discarded.
func (bp *BufferPool) Put(buf []byte) {
	capBuf := cap(buf)
	for _, sz := range bp.sizes {
		if sz == capBuf {
			bp.pools[sz].Put(buf[:sz])
			return
		}
	}
}
