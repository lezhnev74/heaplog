package common

import (
	"context"
	"fmt"
	"hash/crc32"
	"iter"
	"os"
	"os/signal"
	"syscall"
)

var (
	crc32t = crc32.MakeTable(0xD5828281)
)

func WaitSignal() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		cancel()
	}()
	return ctx
}

// ChunksN splits items into n contiguous chunks with sizes as even as possible.
// The first (len(items) % chunks) chunks get one extra element.
func ChunksN[T any](items []T, n int) [][]T {
	if n <= 0 {
		return nil
	}
	out := make([][]T, 0, n)
	L := len(items)
	base := L / n
	rem := L % n

	start := 0
	for i := 0; i < n; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		if end > L {
			end = L
		}
		out = append(out, items[start:end])
		start = end
	}
	return out
}

// HashString is a quick and idempotent hashing
func HashString(s string) string {
	h := crc32.Checksum([]byte(s), crc32t)
	return fmt.Sprintf("%d", h)
}

func Empty[T any]() iter.Seq[T] {
	return func(yield func(T) bool) {}
}

func Empty2[K, V any]() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {}
}
