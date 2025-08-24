package search

import (
	"context"
	"sync"

	"heaplog_2024/internal/common"
)

func NewMatchPool(
	ctx context.Context,
	matcher func(body common.FileMessageBody) bool,
	in <-chan common.FileMessageBody,
	workers int,
) <-chan common.FileMessageBody {
	if workers < 1 {
		panic("empty matching pool (no workers)")
	}
	matched := make(chan common.FileMessageBody, 100)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for m := range in {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if matcher(m) {
					matched <- m
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(matched)
	}()
	return matched
}
