package common

import (
	"context"
	"fmt"
	"hash/crc32"
	"iter"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	crc32t = crc32.MakeTable(0xD5828281)
)

// RepeatEvery executes the given function f repeatedly at specified intervals until the context is cancelled.
// The function is called immediately upon start, then repeatedly after each interval duration.
// The execution runs in a separate goroutine and can be stopped by cancelling the provided context.
func RepeatEvery(ctx context.Context, interval time.Duration, f func()) {
	go func() {
		f() // instant call
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				f()
			}
		}
	}()
}

// ReadMessages converts a sequence of file messages into a sequence of message bodies with their content.
// It efficiently reads message contents from files by reusing file handles when possible.
func ReadMessages(ctx context.Context, messages iter.Seq[FileMessage]) iter.Seq2[FileMessageBody, error] {
	var (
		stream *os.File
		err    error
	)
	return func(yield func(FileMessageBody, error) bool) {
		defer func() {
			if stream != nil {
				stream.Close()
			}
		}()

		for m := range messages {
			if ctx.Err() != nil {
				return
			}

			if stream == nil || stream.Name() != m.File {
				if stream != nil {
					stream.Close()
				}
				stream, err = os.Open(m.File)
				if err != nil {
					yield(FileMessageBody{}, fmt.Errorf("file open %s: %w", m.File, err))
					return
				}
			}

			mLen := m.Loc.To - m.Loc.From
			if mLen < 0 {
				yield(FileMessageBody{}, fmt.Errorf("invalid message location: %d-%d", m.Loc.From, m.Loc.To))
				return
			}
			buf := make([]byte, mLen) // alloc memory for the message
			_, err = stream.ReadAt(buf, int64(m.Loc.From))
			if err != nil {
				yield(FileMessageBody{}, fmt.Errorf("read file %s: %w", m.File, err))
				return
			}

			if !yield(FileMessageBody{FileMessage: m, Body: buf}, nil) {
				return
			}
		}
	}
}
func ToFileMessages(messages iter.Seq[FileMessageBody]) iter.Seq[FileMessage] {
	return func(yield func(FileMessage) bool) {
		for m := range messages {
			if !yield(m.FileMessage) {
				break
			}
		}
	}
}

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
