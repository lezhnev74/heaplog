package common

import "time"

type Segment struct {
	Location
}

type Message struct {
	Location
	Date time.Time
}
