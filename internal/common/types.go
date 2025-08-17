package common

import "time"

type Segment struct {
	Location
}

type Message struct {
	MessageLayout
	Date time.Time
}

type MessageLayout struct {
	Loc     Location // body in the stream
	DateLoc Location // date in the stream
}
