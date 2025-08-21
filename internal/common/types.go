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
	Query    string     `json:"query"`
	FromDate *time.Time `json:"fromDate"`
	ToDate   *time.Time `json:"toDate"`
}

type SearchResult struct {
	UserQuery
	Id        int       `json:"id"` // query id
	Messages  int       `json:"messages"`
	CreatedAt time.Time `json:"createdAt"` // created at
	Finished  bool      `json:"finished"`
}
