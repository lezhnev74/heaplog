package ingest

import (
	"time"

	"heaplog_2024/internal/common"
)

type Segment struct {
	common.Location
}

type Message struct {
	common.Location
	Date time.Time
}
