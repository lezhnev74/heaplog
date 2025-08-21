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

type UserQuery struct {
	Query            string
	FromDate, ToDate *time.Time
}

type SearchResult struct {
	UserQuery
	Id        int // query id
	Messages  int
	CreatedAt time.Time // created at
	Finished  bool
}
