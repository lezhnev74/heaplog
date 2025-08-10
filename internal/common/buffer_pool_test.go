package common

import (
	"testing"
)

func TestBufferPool(t *testing.T) {
	// Initialize the buffer pool with specific sizes
	sizes := []int{128, 256, 512}
	pool := NewBufferPool(sizes)

	// Test getting buffer of exact size
	t.Run("Get buffer of smaller size", func(t *testing.T) {
		buf := pool.Get(64)
		if cap(buf) != 128 {
			t.Errorf("Expected buffer capacity 128, got %d", cap(buf))
		}
		pool.Put(buf)
		buf2 := pool.Get(64)
		if &buf[0] != &buf2[0] {
			t.Error("Buffer was not reused from pool")
		}
	})

	// Test getting buffer of exact size
	t.Run("Get buffer of exact size", func(t *testing.T) {
		buf := pool.Get(128)
		if cap(buf) != 128 {
			t.Errorf("Expected buffer capacity 128, got %d", cap(buf))
		}
		pool.Put(buf)
		buf2 := pool.Get(64)
		if &buf[0] != &buf2[0] {
			t.Error("Buffer was not reused from pool")
		}
	})

	// Test getting buffer of size between pool sizes
	t.Run("Get buffer of intermediate size", func(t *testing.T) {
		buf := pool.Get(200)
		if cap(buf) != 256 {
			t.Errorf("Expected buffer capacity 256, got %d", cap(buf))
		}
		pool.Put(buf)
		buf2 := pool.Get(200)
		if &buf[0] != &buf2[0] {
			t.Error("Buffer was not reused from pool")
		}
	})

	// Test getting buffer larger than any pool size
	t.Run("Get buffer larger than pool sizes", func(t *testing.T) {
		buf := pool.Get(1024)
		if cap(buf) != 1024 {
			t.Errorf("Expected buffer capacity 1024, got %d", cap(buf))
		}
		pool.Put(buf)
		buf2 := pool.Get(1024)
		if &buf[0] == &buf2[0] {
			t.Error("Buffer was reused from pool")
		}
	})
}
