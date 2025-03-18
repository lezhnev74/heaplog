package search

import (
	"fmt"
	"iter"
	"time"

	"heaplog_2024/common"
	"heaplog_2024/db"

	"golang.org/x/exp/mmap"
)

// StreamFileMatch streams matched messages out of the file.
// todo: use iterator for the messages
func StreamFileMatch(file string, messages []db.Message, mf SearchMatcher, dateFormat string) (iter.Seq[common.ErrVal[db.Message]], error) {
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

	return func(yield func(v common.ErrVal[db.Message]) bool) {
		defer stream.Close()

		// Check every message in the messages iterator until one matched
		for {
			ret := common.ErrVal[db.Message]{}
			// experiment: release mmap after reading N bytes
			if mmapScannedBytes > refreshMmapBytes {
				mmapScannedBytes = 0
				_ = stream.Close()
				stream, err = mmap.Open(file)
				if err != nil {
					ret.Err = fmt.Errorf("match message: mmap open: %w", err)
					yield(ret)
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
			if cap(buf) < int(mLen) {
				buf = make([]byte, 0, mLen)
			}
			buf = buf[:mLen]

			n, err = stream.ReadAt(buf, int64(m.Loc.From))
			if err != nil {
				ret.Err = fmt.Errorf("match message: %w", err)
				yield(ret)
				return
			}
			mmapScannedBytes += n

			// parse the date of the message
			var t time.Time
			t, err = time.Parse(dateFormat, string(buf[m.RelDateLoc.From:m.RelDateLoc.From+m.RelDateLoc.To]))
			t = t.UTC()
			if err != nil {
				ret.Err = fmt.Errorf("match message: parse date: %w", err)
				yield(ret)
				return
			}
			m.Date = &t

			matchedMessage := mf(m, buf)
			if len(buf) > maxBufLen {
				buf = nil // release buffer allocated for a big message quickly
			}

			if !matchedMessage {
				continue // bad message
			}

			ret.Val = m
			yield(ret) // good message, return to the iterator
		}
	}, nil
}
