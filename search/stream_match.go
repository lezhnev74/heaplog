package search

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"golang.org/x/exp/mmap"
	"golang.org/x/xerrors"
	"heaplog_2024/db"
	"time"
)

// StreamFileMatch streams matched messages out of the file.
func StreamFileMatch(file string, messages []db.Message, mf SearchMatcher, dateFormat string) (go_iterators.Iterator[db.Message], error) {
	buf := make([]byte, 0, 1000)
	messageIndex := 0
	mmapReader, err := mmap.Open(file)
	if err != nil {
		return nil, xerrors.Errorf("match messages: %w", err)
	}

	it := go_iterators.NewCallbackIterator(
		func() (m db.Message, err error) {

			// Check every message in the messages iterator until one matched
			for {
				if messageIndex == len(messages) {
					break
				}
				m = messages[messageIndex]
				messageIndex++

				mLen := m.Loc.To - m.Loc.From
				if cap(buf) < int(mLen) {
					buf = make([]byte, 0, mLen)
				}
				buf = buf[:mLen]

				_, err = mmapReader.ReadAt(buf, int64(m.Loc.From))
				if err != nil {
					err = xerrors.Errorf("match message: %w", err)
					return
				}

				// parse the date of the message
				var t time.Time
				t, err = time.Parse(dateFormat, string(buf[m.RelDateLoc.From:m.RelDateLoc.From+m.RelDateLoc.To]))
				t = t.UTC()
				if err != nil {
					err = xerrors.Errorf("match message: parse date: %w", err)
					return
				}
				m.Date = &t

				if !mf(m, buf) {
					continue // bad message
				}

				return // good message, return to the iterator
			}

			err = go_iterators.EmptyIterator
			return
		},
		func() error {
			return mmapReader.Close()
		},
	)

	return it, nil
}
