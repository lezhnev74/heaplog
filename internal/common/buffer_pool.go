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

type Buffer struct {
	pool *BufferPool
	Buf  []byte
}

// Close puts the Buf back to the pool
func (b *Buffer) Close() {
	b.pool.Put(b.Buf)
	b.Buf = nil
}

// NewBufferPool creates a new buffer pool with the given slice sizes.
// Each size will have its own sync.Pool instance.
func NewBufferPool(sizes []int) *BufferPool {
	pools := make(map[int]*sync.Pool, len(sizes))
	for _, sz := range sizes {
		size := sz // capture loop variable
		pools[size] = &sync.Pool{
			New: func() any {
				buf := make([]byte, size)
				return &buf // store pointer to slice
			},
		}
	}
	return &BufferPool{pools: pools, sizes: sizes}
}

// Get returns a byte slice with capacity >= minSize.
// If no suitable buffer is found in the pool, a new one is allocated.
func (bp *BufferPool) Get(minSize int) Buffer {
	for _, sz := range bp.sizes {
		if sz >= minSize {
			bufPtr := bp.pools[sz].Get().(*[]byte)
			return Buffer{Buf: (*bufPtr)[:minSize], pool: bp}
		}
	}
	// fallback: exact size (not pooled)
	return Buffer{Buf: make([]byte, minSize), pool: bp}
}

// Put returns a buffer to the pool if its capacity matches one of the predefined sizes.
// Buffers with non-matching sizes are discarded.
func (bp *BufferPool) Put(buf []byte) {
	for _, sz := range bp.sizes {
		if sz == cap(buf) {
			// return full-capacity slice pointer
			b := buf[:cap(buf)]
			bp.pools[sz].Put(&b)
			return
		}
	}
}
