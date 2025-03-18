package db

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"math"
	"time"

	"heaplog_2024/common"

	"github.com/marcboeker/go-duckdb"
)

type QueryDB struct {
	db *sql.DB

	appenderChan chan QueryMessagePacker
	appender     *duckdb.Appender // -> file_segments_messages
}

type Query struct {
	Id       int
	Text     string
	Min, Max *time.Time
	BuiltAt  *time.Time
	// Sets to true when the searching is over.
	Finished bool
	Messages int
}

type QueryMessagePacker struct {
	queryId uint32
	fileId  int
	from    uint64
	len     uint32
	date    time.Time
}

func NewQueryDb(_db *sql.DB, appender *duckdb.Appender) *QueryDB {

	qdb := &QueryDB{
		db:           _db,
		appender:     appender,
		appenderChan: make(chan QueryMessagePacker, 100),
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		for range t.C {
			qdb.Flush() // auto flush
		}
	}()

	go func() {
		var err error
		for mp := range qdb.appenderChan {
			if mp.queryId == 0 {
				err = qdb.appender.Flush()
				if err != nil {
					log.Printf("check in result error: %s", err)
				}
				continue
			} else {
				err = qdb.appender.AppendRow(mp.queryId, uint32(mp.fileId), mp.from, mp.len, uint64(mp.date.UnixMicro()))
			}
			if err != nil {
				log.Printf("check in result error: %s", err)
			}
		}
	}()

	return qdb
}

func (q *QueryDB) RemoveQuery(queryId int) error {
	_, err := q.db.Exec(`DELETE FROM queries WHERE queryId=?`, queryId)
	if err != nil {
		return err
	}
	_, err = q.db.Exec(`DELETE FROM query_results WHERE queryId=?`, queryId)
	return err
}

func (q *QueryDB) ReserveQueryId() (queryId uint32, err error) {
	r := q.db.QueryRow(`SELECT nextval('query_ids');`)
	err = r.Scan(&queryId)
	if err != nil {
		err = fmt.Errorf("unable to check in a query: %w", err)
	}
	return
}

// Flush makes sure all previously checked-in Messages are persisted on disk
func (q *QueryDB) Flush() {
	q.appenderChan <- QueryMessagePacker{} // special mark to flush
}

// CheckinQuery returns queryId instantly while ingesting the Messages.
func (q *QueryDB) CheckinQuery(ctx context.Context, text string, min, max *time.Time, messages iter.Seq[common.ErrVal[Message]]) (query Query, err error) {
	queryId, err := q.ReserveQueryId()
	if err != nil {
		err = fmt.Errorf("query checkin: %w", err)
		return
	}

	var minMicro, maxMicro int64
	if min != nil {
		minMicro = min.UnixMicro()
	}
	if max != nil {
		maxMicro = max.UnixMicro()
	}
	now := time.Now().UnixMicro()

	_, err = q.db.Exec("INSERT INTO queries VALUES (?,?,?,?,?,?,?)", queryId, text, minMicro, maxMicro, 0, false, now)
	if err != nil {
		err = fmt.Errorf("query checkin: %w", err)
		return
	}

	// load all query Messages from the iterator in the background
	// return query Id instantly to start listening for the first result.
	go func() {
		defer func() {
			if err != nil {
				log.Printf("query %d: checkin query defer: %s", queryId, err)
			}

			q.appenderChan <- QueryMessagePacker{} // ask for flush

			// mark as Finished
			_, err = q.db.Exec("UPDATE queries SET finished=true WHERE queryId=?", queryId)
			if err != nil {
				log.Printf("query %d: checkin query defer: %s", queryId, err)
			}
		}()

		var (
			n int
		)
		for ev := range messages {
			// Cancellation test:
			select {
			case <-ctx.Done():
				return
			default:
			}

			if ev.Err != nil {
				log.Printf("query %d: checkin query: %s", queryId, ev.Err)
				return
			}

			q.appenderChan <- QueryMessagePacker{queryId, ev.Val.FileId, ev.Val.Loc.From, uint32(ev.Val.Loc.Len()), *ev.Val.Date}

			n++
		}

		if n > 0 {
			// after the ingestion is over, update segment's upper boundary
			// lastDate is null if there were no messages
			_, err = q.db.Exec("UPDATE queries SET messages=? WHERE queryId=?", n, queryId)
			if err != nil {
				log.Printf("query %d: query finish: %s", queryId, err)
				return
			}
		}
	}()

	return q.FindQuery(int(queryId))
}

func (q *QueryDB) FindQuery(queryId int) (query Query, err error) {
	query.Id = queryId
	r := q.db.QueryRow("SELECT text,dateMin,dateMax,builtAt,finished,messages FROM queries WHERE queryId=?", queryId)

	var dateMin, dateMax, builtAt int64
	err = r.Scan(&query.Text, &dateMin, &dateMax, &builtAt, &query.Finished, &query.Messages)
	if err != nil {
		err = fmt.Errorf("read query: %w", err)
		return
	}

	if dateMin > 0 {
		t := time.UnixMicro(dateMin)
		query.Min = &t
	}
	if dateMax > 0 {
		t := time.UnixMicro(dateMax)
		query.Max = &t
	}
	t := time.UnixMicro(builtAt)
	query.BuiltAt = &t

	return
}

// Count returns how many messages exist in a sub-query (query with time constraints)
func (q *QueryDB) Count(queryId int, min, max *time.Time) (c int, err error) {
	sqlSelect := `
	SELECT COUNT(*) 
	FROM query_results
	WHERE queryId=? AND date>=? and date<=?
`
	minMicro := int64(0)
	maxMicro := int64(math.MaxInt64)
	if min != nil {
		minMicro = min.UnixMicro()
	}
	if max != nil {
		maxMicro = max.UnixMicro()
	}

	r := q.db.QueryRow(sqlSelect, queryId, minMicro, maxMicro)
	err = r.Scan(&c)
	return
}

// Page is based on offsets, so further optimization is possible
func (q *QueryDB) Page(queryId int, min, max *time.Time, page, pageLen int) (messages []Message, err error) {
	sqlSelect := `
	SELECT fileId,pos,len 
	FROM query_results
	WHERE queryId=? AND date>=? and date<=?
	ORDER BY date ASC -- show early messages first (just like in a file)
	LIMIT ? OFFSET ? -- use cursors based on rowid instead?
`
	minMicro := int64(0)
	maxMicro := int64(math.MaxInt64)
	if min != nil {
		minMicro = min.UnixMicro()
	}
	if max != nil {
		maxMicro = max.UnixMicro()
	}

	r, err := q.db.Query(sqlSelect, queryId, minMicro, maxMicro, pageLen, page*pageLen)
	if err != nil {
		return
	}
	defer func() { _ = r.Close() }()

	for r.Next() {
		m := Message{}
		err = r.Scan(&m.FileId, &m.Loc.From, &m.Loc.To)
		if err != nil {
			return
		}
		messages = append(messages, m)
	}

	return
}

// Stream all messages for a query.
// If the query is still in-flight, it will push messages as they appear.
func (q *QueryDB) Stream(queryId int, min, max *time.Time) (messages iter.Seq[common.ErrVal[Message]]) {

	// confirm query has finished twice as these are two separate query ops
	doubleConfirm := 0
	offset := 0

	sqlSelect := `
	SELECT fileId,pos,len 
	FROM query_results
	WHERE queryId=? AND date>=? and date<=?
	ORDER BY date ASC -- show early messages first (just like in a file)
	OFFSET ? -- to read out more messages as they appear
`
	minMicro := int64(0)
	maxMicro := int64(math.MaxInt64)
	if min != nil {
		minMicro = min.UnixMicro()
	}
	if max != nil {
		maxMicro = max.UnixMicro()
	}

	return func(yield func(val common.ErrVal[Message]) bool) {
		var ret common.ErrVal[Message]
		var r *sql.Rows

		// Keep reading until the query is finished (double confirm) or we get a batch of rows
		for {
			if r != nil {
				_ = r.Close()
			}
			r, ret.Err = q.db.Query(sqlSelect, queryId, minMicro, maxMicro, offset)
			defer func() { _ = r.Close() }()

			if ret.Err != nil {
				yield(ret)
				return
			}

			for r.Next() {
				ret.Err = r.Scan(&ret.Val.FileId, &ret.Val.Loc.From, &ret.Val.Loc.To)
				if !yield(ret) {
					return
				}
				if ret.Err != nil {
					return
				}
				offset++
			}

			// here we see no more rows
			// maybe the query is still fetching rows, or maybe it has no data, test the query itself
			query, err := q.FindQuery(queryId)
			if err != nil {
				ret.Err = err
				yield(ret)
				return
			}

			if query.Finished {
				doubleConfirm++
				if doubleConfirm >= 2 {
					return
				}
			}

			// Sleep before trying to query rows again
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (q *QueryDB) List() (queries []Query, err error) {
	rows, err := q.db.Query("SELECT queryId,text,dateMin,dateMax,builtAt,finished,messages FROM queries ORDER BY queryId desc")
	if err != nil {
		err = fmt.Errorf("list query: %w", err)
		return
	}

	var dateMin, dateMax, builtAt int64
	for rows.Next() {
		query := Query{}
		err = rows.Scan(&query.Id, &query.Text, &dateMin, &dateMax, &builtAt, &query.Finished, &query.Messages)
		if err != nil {
			err = fmt.Errorf("read query: %w", err)
			return
		}

		if dateMin > 0 {
			t := time.UnixMicro(dateMin)
			query.Min = &t
		}
		if dateMax > 0 {
			t := time.UnixMicro(dateMax)
			query.Max = &t
		}
		t := time.UnixMicro(builtAt)
		query.BuiltAt = &t

		queries = append(queries, query)
	}

	return
}
