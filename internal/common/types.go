package common

import "time"

type Segment struct {
	Location
}

type FileMessageBody struct {
	FileMessage
	Body []byte
}

type FileMessage struct {
	File string
	Message
}

type Message struct {
	MessageLayout
	Date time.Time
}

type MessageLayout struct {
	Loc     Location // body in the stream
	DateLoc Location // date in the stream
}
