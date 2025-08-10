package ingest

import (
	"time"

	"heaplog_2024/internal/common"
)

type Segment struct {
	Loc common.Location
}

type Message struct {
	Loc  common.Location
	Date time.Time
}
