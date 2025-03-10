package db

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math"
	"time"

	"github.com/lezhnev74/go-iterators"
	"github.com/marcboeker/go-duckdb"
	"golang.org/x/xerrors"
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
		err = xerrors.Errorf("unable to check in a query: %w", err)
	}
	return
}

// Flush makes sure all previously checked-in Messages are persisted on disk
func (q *QueryDB) Flush() {
	q.appenderChan <- QueryMessagePacker{} // special mark to flush
}

// CheckinQuery returns queryId instantly while ingesting the Messages.
func (q *QueryDB) CheckinQuery(ctx context.Context, text string, min, max *time.Time, messages go_iterators.Iterator[Message]) (query Query, err error) {
	queryId, err := q.ReserveQueryId()
	if err != nil {
		err = xerrors.Errorf("query checkin: %w", err)
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
		err = xerrors.Errorf("query checkin: %w", err)
		return
	}

	// load all query Messages from the iterator in the background
	// return query Id instantly to start listening for the first result.
	go func() {
		defer func() {
			err = messages.Close()
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
			m Message
			n int
		)
		for {
			// Cancellation test:
			select {
			case <-ctx.Done():
				return
			default:
			}

			m, err = messages.Next()
			if errors.Is(err, go_iterators.EmptyIterator) {
				break
			} else if err != nil {
				log.Printf("query %d: checkin query: %s", queryId, err)
				return
			}

			q.appenderChan <- QueryMessagePacker{queryId, m.FileId, m.Loc.From, uint32(m.Loc.Len()), *m.Date}

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
		err = xerrors.Errorf("read query: %w", err)
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
func (q *QueryDB) Stream(queryId int, min, max *time.Time) (messages go_iterators.Iterator[Message], err error) {

	totalRead := 0
	// confirm query has finished twice as these are two separate query ops
	doubleConfirm := 0

	readBatch := func() (messages []Message, err error) {
		sqlSelect := `
	SELECT fileId,pos,len 
	FROM query_results
	WHERE queryId=? AND date>=? and date<=?
	ORDER BY date ASC -- show early messages first (just like in a file)
	OFFSET ?
`
		minMicro := int64(0)
		maxMicro := int64(math.MaxInt64)
		if min != nil {
			minMicro = min.UnixMicro()
		}
		if max != nil {
			maxMicro = max.UnixMicro()
		}

		// Keep reading until the query is finished (double confirm) or we get a batch of rows
		for {
			r, err := q.db.Query(sqlSelect, queryId, minMicro, maxMicro, totalRead)
			if err != nil {
				return nil, err
			}
			defer func() { _ = r.Close() }()

			for r.Next() {
				m := Message{}
				err = r.Scan(&m.FileId, &m.Loc.From, &m.Loc.To)
				if err != nil {
					return nil, err
				}
				messages = append(messages, m)
				totalRead++
			}

			if len(messages) == 0 {
				// maybe the query is still fetching rows, or maybe it has no data, test the query itself
				query, err := q.FindQuery(queryId)
				if err != nil {
					return nil, err
				}
				if query.Finished {
					doubleConfirm++
					if doubleConfirm >= 2 {
						return messages, nil
					}
				}
				time.Sleep(time.Millisecond * 100) // no data available and query is still in-flight
				continue                           // read again
			}

			break // return messages that we got so far
		}

		return
	}

	retIterator := go_iterators.NewDynamicSliceIterator(readBatch, func() error { return nil })
	return retIterator, nil
}

func (q *QueryDB) List() (queries []Query, err error) {
	rows, err := q.db.Query("SELECT queryId,text,dateMin,dateMax,builtAt,finished,messages FROM queries ORDER BY queryId desc")
	if err != nil {
		err = xerrors.Errorf("list query: %w", err)
		return
	}

	var dateMin, dateMax, builtAt int64
	for rows.Next() {
		query := Query{}
		err = rows.Scan(&query.Id, &query.Text, &dateMin, &dateMax, &builtAt, &query.Finished, &query.Messages)
		if err != nil {
			err = xerrors.Errorf("read query: %w", err)
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
