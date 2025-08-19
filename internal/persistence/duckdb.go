package persistence

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	_ "github.com/marcboeker/go-duckdb/v2"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search"
)

//go:embed migrations/_.sql
var migrationFS embed.FS

type DuckDB struct {
	db           *sql.DB
	queryResults *Appender
	logger       *zap.Logger
}

func NewDuckDB(ctx context.Context, filePath string, logger *zap.Logger) (duck *DuckDB, err error) {
	duck = &DuckDB{
		logger: logger,
	}

	c, err := duckdb.NewConnector(filePath, nil)
	if err != nil {
		err = fmt.Errorf("could not initialize new connector: %w", err)
		return
	}

	con, err := c.Connect(context.Background())
	if err != nil {
		err = fmt.Errorf("could not connect: %w", err)
	}
	duck.db = sql.OpenDB(c)

	// must migrate before making an appender
	err = duck.Migrate()
	if err != nil {
		err = fmt.Errorf("could not migrate: %w", err)
	}

	queryResultsAppender, err := duckdb.NewAppenderFromConn(con, "", "query_results")
	if err != nil {
		err = fmt.Errorf("could not create new appender for query_results: %w", err)
		return
	}
	duck.queryResults = NewAppender(queryResultsAppender)

	go func() {
		<-ctx.Done()
		duck.queryResults.Close()
		duck.Close()
	}()

	return duck, nil
}

func (duck *DuckDB) PutResultsAsync(query string, results iter.Seq[common.FileMessage]) (
	result search.SearchResult,
	done chan struct{},
	err error,
) {
	var queryId int
	now := time.Now().UTC()

	tx, err := duck.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	err = duck.db.QueryRow(`SELECT nextval('query_ids')`).Scan(&queryId)
	if err != nil {
		return
	}

	_, err = tx.Exec(
		"INSERT INTO queries (queryId, text, date_min, date_max, messages, finished, built_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		queryId, query, 0, 0, 0, false, now.UnixMicro(),
	)
	if err != nil {
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	result = search.SearchResult{
		Id:       queryId,
		Query:    query,
		Date:     time.UnixMicro(now.UnixMicro()).UTC(),
		Messages: 0,
		Finished: false,
	}

	done = make(chan struct{})
	go func() {
		defer close(done)
		var (
			messages         int
			minDate, maxDate int64 = math.MaxInt64, 0
			err              error
		)
		for msg := range results {
			messages++
			dateMicro := msg.Date.UnixMicro()
			if dateMicro < minDate {
				minDate = dateMicro
			}
			if dateMicro > maxDate {
				maxDate = dateMicro
			}

			fileId, err := duck.getFileIdByPath(msg.File)
			if err != nil {
				break
			}

			err = duck.queryResults.AppendRow(
				queryId,
				fileId,
				msg.Loc.From,
				msg.Loc.To-msg.Loc.From,
				dateMicro,
			)
			if err != nil {
				duck.logger.Error("could not append row to query_results", zap.Error(err))
				break
			}
		}

		err = duck.queryResults.Flush()
		if err != nil {
			return
		}

		_, err = duck.db.Exec(
			"UPDATE queries SET messages = ?, date_min = ?, date_max = ?, finished = ? WHERE queryId = ?",
			messages, minDate, maxDate, true, queryId,
		)
	}()

	return
}

func (duck *DuckDB) GetResultMessages(resultId int) (iter.Seq[common.FileMessage], error) {
	q := `
		SELECT files.path, pos, len, date
		FROM query_results
		JOIN files ON files.id = file_id
		WHERE query_id = ?
		ORDER BY date
	`
	rows, err := duck.db.Query(q, resultId)
	if err != nil {
		return nil, err
	}

	return func(yield func(common.FileMessage) bool) {
		defer rows.Close()
		for rows.Next() {
			msg := common.FileMessage{}
			var dateMicro int64
			err = rows.Scan(&msg.File, &msg.Loc.From, &msg.Loc.To, &dateMicro)
			if err != nil {
				panic(err)
			}
			msg.Date = time.UnixMicro(dateMicro).UTC()
			msg.Loc.To += msg.Loc.From // Convert length to absolute position
			if !yield(msg) {
				break
			}
		}
	}, nil
}

func (duck *DuckDB) GetResults(resultId int) (search.SearchResult, error) {
	var r search.SearchResult
	var builtAt int64
	err := duck.db.QueryRow(
		`
		SELECT queryId, text, built_at, messages, finished 
		FROM queries 
		WHERE queryId = ?
		`,
		resultId,
	).Scan(&r.Id, &r.Query, &builtAt, &r.Messages, &r.Finished)
	if err != nil {
		return r, err
	}
	r.Date = time.UnixMicro(builtAt).UTC()
	return r, nil
}

func (duck *DuckDB) GetAllResults() ([]search.SearchResult, error) {
	rows, err := duck.db.Query(
		`
		SELECT queryId, text, built_at, messages, finished 
		FROM queries 
		ORDER BY built_at DESC
	`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []search.SearchResult
	for rows.Next() {
		var r search.SearchResult
		var builtAt int64
		if err := rows.Scan(&r.Id, &r.Query, &builtAt, &r.Messages, &r.Finished); err != nil {
			return nil, err
		}
		r.Date = time.UnixMicro(builtAt).UTC()
		results = append(results, r)
	}
	return results, rows.Err()
}

func (duck *DuckDB) WipeResults(resultId int) error {
	tx, err := duck.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM queries WHERE queryId = ?", resultId)
	if err != nil {
		return err
	}
	_, err = tx.Exec("DELETE FROM query_results WHERE query_id = ?", resultId)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (duck *DuckDB) Close() (err error) { return duck.db.Close() }

func (duck *DuckDB) getFileIdByPath(path string) (id int, err error) {
	tx, err := duck.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	err = duck.db.QueryRow("SELECT id FROM files WHERE path = ?", path).Scan(&id)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		err = duck.db.QueryRow(`SELECT nextval('file_ids')`).Scan(&id)
		if err != nil {
			return
		}
		_, err = duck.db.Exec("INSERT INTO files (id, path) VALUES (?,?)", id, path)
		if err != nil {
			return
		}
	} else if err != nil {
		return
	}

	err = tx.Commit()
	return
}
func (duck *DuckDB) Migrate() (err error) {
	f, err := migrationFS.Open("migrations/_.sql")
	if err != nil {
		return err
	}
	migrateContent, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	_, err = duck.db.Exec(string(migrateContent))
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil // skip migrations
	}
	return err
}

func (duck *DuckDB) GetSegments() (map[string][]common.Location, error) {
	q := `
		SELECT files.path, segments.pos_from, segments.pos_to 
		FROM segments
		JOIN files ON files.id=segments.file_id
		ORDER BY files.id, segments.pos_from -- sort by pos(!)
`
	rows, err := duck.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]common.Location)
	for rows.Next() {
		var file string
		var loc common.Location
		if err = rows.Scan(&file, &loc.From, &loc.To); err != nil {
			return nil, err
		}
		result[file] = append(result[file], loc)
	}
	return result, rows.Err()
}

func (duck *DuckDB) PutSegment(file string, messages []common.Message) (segmentId int, err error) {
	if len(messages) == 0 {
		err = fmt.Errorf("no messages in segment")
		return
	}

	tx, err := duck.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	fileId, err := duck.getFileIdByPath(file)
	if err != nil {
		err = fmt.Errorf("get file id: %w", err)
		return
	}
	err = duck.db.QueryRow(`SELECT nextval('segment_ids')`).Scan(&segmentId)
	if err != nil {
		return
	}
	_, err = tx.Exec(
		"INSERT INTO segments (id, file_id, pos_from, pos_to, date_min, date_max) VALUES (?, ?, ?, ?, ?, ?)",
		segmentId,
		fileId,
		messages[0].Loc.From,
		messages[len(messages)-1].Loc.To,
		messages[0].Date.UnixMicro(),
		messages[len(messages)-1].Date.UnixMicro(),
	)
	if err != nil {
		return
	}

	for _, msg := range messages {
		_, err = tx.Exec(
			"INSERT INTO messages (segment_id, rel_from, rel_date_from, rel_date_to, date) VALUES (?, ?, ?, ?, ?)",
			segmentId,
			msg.Loc.From-messages[0].Loc.From,
			msg.DateLoc.From,
			msg.DateLoc.To,
			msg.Date.UnixMicro(),
		)
		if err != nil {
			return
		}
	}

	err = tx.Commit()
	return
}

func (duck *DuckDB) WipeSegment(file string, segment common.Location) error {
	tx, err := duck.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileId, err := duck.getFileIdByPath(file)
	if err != nil {
		return err
	}

	var segmentId int
	err = duck.db.QueryRow(
		"SELECT id FROM segments WHERE file_id = ? AND pos_from = ? AND pos_to = ?",
		fileId, segment.From, segment.To,
	).Scan(&segmentId)
	if err != nil {
		return err
	}

	err = duck._txDeleteSegment(tx, segmentId)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (duck *DuckDB) WipeSegments(file string) error {
	tx, err := duck.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileId, err := duck.getFileIdByPath(file)
	if err != nil {
		return err
	}

	rows, err := duck.db.Query("SELECT id FROM segments WHERE file_id = ?", fileId)
	if err != nil {
		return err
	}
	for rows.Next() {
		var segmentId int
		err = rows.Scan(&segmentId)
		if err != nil {
			return err
		}
		err = duck._txDeleteSegment(tx, segmentId)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (duck *DuckDB) _txDeleteSegment(tx *sql.Tx, segmentId int) error {
	_, err := tx.Exec("DELETE FROM segments WHERE id = ?", segmentId)
	if err != nil {
		return err
	}
	_, err = tx.Exec("DELETE FROM messages WHERE segment_id = ?", segmentId)
	if err != nil {
		return err
	}
	return nil
}

func (duck *DuckDB) WipeFile(file string) error {
	tx, err := duck.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = duck.WipeSegments(file)
	if err != nil {
		return err
	}
	_, err = duck.db.Exec("DELETE FROM files WHERE path = ?", file)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Main gateway for getting messages from the database.
func (duck *DuckDB) GetMessages(segments []int, minDate, maxDate *time.Time) (iter.Seq[common.FileMessage], error) {

	minMicro, maxMicro := int64(0), int64(math.MaxInt64)
	if minDate != nil {
		minMicro = minDate.UnixMicro()
	}
	if maxDate != nil {
		maxMicro = maxDate.UnixMicro()
	}

	q := `
	SELECT
	    files.path,
	    
	    segments.id,
	    segments.pos_from,
	    segments.pos_to,
	    
	    messages.rel_from,
	    messages.rel_date_from,
	    messages.rel_date_to,
	    messages.date
	FROM messages
	JOIN segments on segments.id=messages.segment_id 
	JOIN files on files.id=segments.file_id 
	WHERE messages.date >= ? AND messages.date <= ? AND %s
	ORDER BY messages.date
	`
	if len(segments) > 0 {
		q = fmt.Sprintf(q, "segments.id IN ("+strings.Repeat("?,", len(segments)-1)+"?)")
	} else {
		q = fmt.Sprintf(q, "1=1")
	}

	rows, err := duck.db.Query(
		q,
		append([]any{minMicro, maxMicro}, asAny(segments)...)...,
	)
	if err != nil {
		return nil, err
	}

	// Return an iterator over all messages in the given file path, ordered by date.
	// Messages are yielded one by one with correct To positions calculated from the next message.
	return func(yield func(common.FileMessage) bool) {
		defer rows.Close()

		var (
			prevMessage                                                 *common.FileMessage
			prevSegmentId, segmentId, segmentFrom, segmentTo, dateMicro int
		)
		for rows.Next() {
			cur := common.FileMessage{}
			err = rows.Scan(
				&cur.File,

				&segmentId,
				&segmentFrom,
				&segmentTo,

				&cur.Loc.From,
				&cur.DateLoc.From,
				&cur.DateLoc.To,
				&dateMicro,
			)
			if err != nil {
				panic(err)
			}

			cur.Date = time.UnixMicro(int64(dateMicro)).UTC()
			cur.Loc.From += segmentFrom
			cur.Loc.To = segmentTo

			if err != nil {
				panic(err)
			}
			if prevMessage == nil {
				// first message, continue to the next for position calculation
				prevMessage = &cur
				prevSegmentId = segmentId
				continue
			}

			if prevSegmentId != segmentId {
				// segments boundary reached
				if !yield(*prevMessage) {
					break
				}
				prevSegmentId = segmentId
				prevMessage = &cur
				continue
			}

			prevMessage.Loc.To = cur.Loc.From
			if !yield(*prevMessage) {
				break
			}
			prevMessage = &cur
		}

		if prevMessage != nil {
			// one message is remaining
			yield(*prevMessage)
		}
	}, nil
}
