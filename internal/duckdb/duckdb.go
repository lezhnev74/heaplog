package duckdb

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

	_ "github.com/marcboeker/go-duckdb/v2"

	"heaplog_2024/internal/common"
)

//go:embed migrations/_.sql
var migrationFS embed.FS

type DuckDB struct {
	db *sql.DB
}

func NewDuckDB(ctx context.Context, dir string) (*DuckDB, error) {
	var err error
	duck := &DuckDB{}
	duck.db, err = sql.Open("duckdb", dir)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		duck.Close() // Flush buffers
	}()
	return duck, nil
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

func (duck *DuckDB) getSegments() (map[string][]common.Location, error) {
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

func (duck *DuckDB) PutSegment(file string, terms [][]byte, messages []common.Message) (segmentId int, err error) {
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
			msg.DateLoc.From-messages[0].Loc.From,
			msg.DateLoc.To-messages[0].Loc.From,
			msg.Date.UnixMicro(),
		)
		if err != nil {
			return
		}
	}

	err = tx.Commit()
	return
}

func (duck *DuckDB) wipeSegment(file string, segment common.Location) error {

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

	_, err = duck.db.Exec("DELETE FROM segments WHERE id = ?", segmentId)
	if err != nil {
		return err
	}

	_, err = duck.db.Exec("DELETE FROM messages WHERE segment_id = ?", segmentId)
	if err != nil {
		return err
	}

	// todo II

	return nil
}

func (duck *DuckDB) wipeSegments(file string) error {
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
		_, err = duck.db.Exec("DELETE FROM segments WHERE id = ?", segmentId)
		if err != nil {
			return err
		}
		_, err = duck.db.Exec("DELETE FROM messages WHERE segment_id = ?", segmentId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (duck *DuckDB) wipeFile(file string) error {
	tx, err := duck.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = duck.wipeSegments(file)
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
			cur.DateLoc.From += segmentFrom
			cur.DateLoc.To += segmentFrom

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
