package search

import (
	"fmt"
	"iter"

	"golang.org/x/exp/mmap"

	"heaplog_2024/internal/common"
)

type SearchMatcher func(m common.Message, body []byte) bool

// StreamFileMatch streams matched messages out of the file.
func StreamFileMatch(
	file string,
	messages []common.Message,
	mf SearchMatcher,
) (iter.Seq2[common.Message, error], error) {
	buf := make([]byte, 0, 1000)
	maxBufLen := 10_000_000

	messageIndex := 0
	stream, err := mmap.Open(file)
	if err != nil {
		return nil, fmt.Errorf("match messages: mmap open: %w", err)
	}

	mmapScannedBytes := 0
	refreshMmapBytes := 500_000_000
	n := 0

	return func(yield func(common.Message, error) bool) {
		defer stream.Close()

		// Check every message in the messages iterator until one matched
		for {
			// experiment: release mmap after reading N bytes
			if mmapScannedBytes > refreshMmapBytes {
				mmapScannedBytes = 0
				_ = stream.Close()
				stream, err = mmap.Open(file)
				if err != nil {
					yield(common.Message{}, fmt.Errorf("match message: mmap open: %w", err))
					return
				}
			}
			////////////////////////////////////////////////////

			if messageIndex == len(messages) {
				break
			}
			m := messages[messageIndex]
			messageIndex++

			mLen := m.Loc.To - m.Loc.From
			if cap(buf) < mLen {
				buf = make([]byte, 0, mLen)
			}
			buf = buf[:mLen]

			n, err = stream.ReadAt(buf, int64(m.Loc.From))
			if err != nil {
				yield(common.Message{}, fmt.Errorf("match message: %w", err))
				return
			}
			mmapScannedBytes += n

			matchedMessage := mf(m, buf)
			if len(buf) > maxBufLen {
				buf = nil // release buffer allocated for a big message quickly
			}

			if !matchedMessage {
				continue // bad message
			}

			yield(m, nil) // good message, return to the iterator
		}
	}, nil
}
