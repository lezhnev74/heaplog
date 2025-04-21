package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/marcboeker/go-duckdb"

	"heaplog_2024/common"
)

type MessagesDb struct {
	db *sql.DB

	appenderChan chan MessageAppendPacket
	appender     *duckdb.Appender // -> file_segments_messages
}

type Message struct {
	SegmentId  uint32
	Loc        common.Location
	RelDateLoc common.Location // relative to the message start

	// optional:
	FileId int
	Date   *time.Time
}

type MessageAppendPacket struct {
	segmentId            uint32
	from                 uint64
	relDateFrom, dateLen uint8
}

var flushPacket = MessageAppendPacket{0, 0, 0, 0}

func NewMessagesDb(db *sql.DB, appender *duckdb.Appender) *MessagesDb {

	mdb := &MessagesDb{
		db:           db,
		appender:     appender,
		appenderChan: make(chan MessageAppendPacket, 10_000),
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		for range t.C {
			mdb.Flush() // auto flush
		}
	}()

	go func() {
		var err error
		for mp := range mdb.appenderChan {
			if mp == flushPacket {
				err = mdb.appender.Flush()
				if err != nil {
					log.Printf("check in message error: %s", err)
				}
				continue
			}

			err = mdb.appender.AppendRow(mp.segmentId, mp.from, mp.relDateFrom, mp.dateLen)
			if err != nil {
				log.Printf("check in message error: %s", err)
			}
		}
	}()

	return mdb
}

// CheckinMessage quickly pushes the message to the database
// But the Messages is not visible unless flushed
func (mdb *MessagesDb) CheckinMessage(segmentId uint32, from uint64, relDateFrom, dateLen uint8) error {
	// The appender seems to be non-thread-safe, so
	// ingestion is done through the channel to avoid data-races.
	mdb.appenderChan <- MessageAppendPacket{segmentId, from, relDateFrom, dateLen}
	return nil
}

// Flush makes sure all previously checked-in Messages are persisted on disk
func (mdb *MessagesDb) Flush() {
	mdb.appenderChan <- flushPacket
}

func (mdb *MessagesDb) AllMessagesIt() (messages go_iterators.Iterator[Message], err error) {
	whereSql := `1=1`
	args := []any{}
	return mdb.iterateRows(whereSql, args)
}

func (mdb *MessagesDb) AllMessagesInFileIt(fileId int) (messages go_iterators.Iterator[Message], err error) {
	whereSql := `s.fileId=?`
	args := []any{fileId}
	return mdb.iterateRows(whereSql, args)
}

func (mdb *MessagesDb) AllMessagesInSegmentsIt(segments []uint32) (messages go_iterators.Iterator[Message], err error) {
	whereSql := `m.segmentId IN (%s)`
	whereSql = fmt.Sprintf(whereSql, strings.TrimRight(strings.Repeat("?,", len(segments)), ","))
	args := common.SliceToAny(segments)
	return mdb.iterateRows(whereSql, args)
}

func (mdb *MessagesDb) AllMessages(fileId int) (messages []Message, err error) {
	it, err := mdb.AllMessagesInFileIt(fileId)
	return go_iterators.ToSlice(it), err
}

// iterateRows gives row iterator and ability to change the query with
// whereSql/queryArgs parameters.
func (mdb *MessagesDb) iterateRows(whereSql string, queryArgs []any) (go_iterators.Iterator[Message], error) {

	sqlSelect := `
		SELECT m.*, s.posTo as lastMessageTo, s.fileId   
		FROM file_segments_messages m
		JOIN file_segments s ON m.segmentId=s.Id
		WHERE %s
		ORDER BY s.DateMin, m.posFrom ASC -- sort by time 
		`
	sqlSelect = fmt.Sprintf(sqlSelect, whereSql)

	r, err := mdb.db.Query(sqlSelect, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("all Messages: %w", err)
	}

	// Messages do not keep their len(or end position),
	// so it must be calculated against the next message or the segment's right boundary.

	// Read the first row to the memory before proceeding
	var lastSegmentPosTo, segmentPosTo uint64
	var lastMessage *Message
	if r.Next() {
		lastMessage = &Message{}
		err = r.Scan(
			&lastMessage.SegmentId,
			&lastMessage.Loc.From,
			&lastMessage.RelDateLoc.From,
			&lastMessage.RelDateLoc.To,
			&lastSegmentPosTo,
			&lastMessage.FileId,
		)
		if err != nil {
			err = fmt.Errorf("all Messages: %w", err)
			return go_iterators.NewSliceIterator[Message](nil), err
		}
	}

	it := go_iterators.NewCallbackIterator(
		func() (m Message, err error) {
			if lastMessage == nil {
				err = go_iterators.EmptyIterator
				return
			}

			segmentPosTo, m = lastSegmentPosTo, *lastMessage

			if r.Next() {
				err = r.Scan(
					&lastMessage.SegmentId,
					&lastMessage.Loc.From,
					&lastMessage.RelDateLoc.From,
					&lastMessage.RelDateLoc.To,
					&lastSegmentPosTo,
					&lastMessage.FileId,
				)
				if err != nil {
					err = fmt.Errorf("all Messages: %w", err)
					return
				}

				// one more message exists, so update the boundary of the current message
				if m.SegmentId == lastMessage.SegmentId {
					m.Loc.To = lastMessage.Loc.From
				} else {
					m.Loc.To = segmentPosTo
				}
			} else {
				// no more messages are coming
				m.Loc.To = segmentPosTo
				lastMessage = nil
			}

			return
		},
		func() error {
			return r.Close()
		},
	)

	return it, nil
}

// IterateRowsFromStatement gives row iterator and ability to change the query with
// whereSql/queryArgs parameters.
func (mdb *MessagesDb) IterateRowsFromStatement(stmt *sql.Stmt, args []any) (go_iterators.Iterator[Message], error) {

	r, err := stmt.Query(args...)
	if err != nil {
		return nil, fmt.Errorf("all Messages: %w", err)
	}

	// Messages do not keep their len(or end position),
	// so it must be calculated against the next message or the segment's right boundary.

	// Read the first row to the memory before proceeding
	var lastSegmentPosTo, segmentPosTo uint64
	var lastMessage *Message
	if r.Next() {
		lastMessage = &Message{}
		err = r.Scan(
			&lastMessage.SegmentId,
			&lastMessage.Loc.From,
			&lastMessage.RelDateLoc.From,
			&lastMessage.RelDateLoc.To,
			&lastSegmentPosTo,
			&lastMessage.FileId,
		)
		if err != nil {
			err = fmt.Errorf("all Messages: %w", err)
			return go_iterators.NewSliceIterator[Message](nil), err
		}
	}

	it := go_iterators.NewCallbackIterator(
		func() (m Message, err error) {
			if lastMessage == nil {
				err = go_iterators.EmptyIterator
				return
			}

			segmentPosTo, m = lastSegmentPosTo, *lastMessage

			if r.Next() {
				err = r.Scan(
					&lastMessage.SegmentId,
					&lastMessage.Loc.From,
					&lastMessage.RelDateLoc.From,
					&lastMessage.RelDateLoc.To,
					&lastSegmentPosTo,
					&lastMessage.FileId,
				)
				if err != nil {
					err = fmt.Errorf("all Messages: %w", err)
					return
				}

				// one more message exists, so update the boundary of the current message
				if m.SegmentId == lastMessage.SegmentId {
					m.Loc.To = lastMessage.Loc.From
				} else {
					m.Loc.To = segmentPosTo
				}
			} else {
				// no more messages are coming
				m.Loc.To = segmentPosTo
				lastMessage = nil
			}

			return
		},
		func() error {
			return r.Close()
		},
	)

	return it, nil
}
