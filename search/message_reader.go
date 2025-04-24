package search

import (
	"errors"
	"os"

	"heaplog_2024/common"
)

// MessageAddr describes one message's bytes in a file
type MessageAddr struct {
	filePath string
	loc      common.Location
}

// ReadMessages simply reads out messages from heap files
func ReadMessages(mAddrs []MessageAddr) (messages [][]byte, err error) {
	batches := common.GroupSlice(mAddrs, func(m MessageAddr) string { return m.filePath })

	processBatch := func(batch []MessageAddr) (messages [][]byte, err error) {
		filePath := batch[0].filePath
		f, err := os.Open(filePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// well, the file has been removed, so do nothign here
				common.Out("unable to read messages for a removed file: %s", filePath)
				err = nil
			}
			return
		}
		defer func() { _ = f.Close() }()

		for _, addr := range batch {
			messageLen := addr.loc.To - addr.loc.From
			buf := make([]byte, messageLen)

			_, err = f.ReadAt(buf, int64(addr.loc.From))
			if err != nil {
				return
			}
			messages = append(messages, buf)
		}
		return
	}

	for _, batch := range batches {
		batchMessages, err2 := processBatch(batch)
		if err2 != nil {
			err = err2
			return
		}
		messages = append(messages, batchMessages...)
	}

	return
}
