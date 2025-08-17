package duckdb

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"iter"
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
		SELECT file.path, segments.pos_from, segments.pos_to 
		FROM segments
		JOIN files ON files.id=segments.file_id
		ORDER BY file.id, segments.pos_from -- sort by pos(!)
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

func (duck *DuckDB) PutSegment(file string, terms [][]byte, messages []common.Message) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := duck.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileId, err := duck.getFileIdByPath(file)
	if err != nil {
		return fmt.Errorf("get file id: %w", err)
	}
	segmentId := 0
	err = duck.db.QueryRow(`SELECT nextval('segment_ids')`).Scan(&segmentId)
	if err != nil {
		return err
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
		return err
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
			return err
		}
	}

	return tx.Commit()
}

func (duck *DuckDB) wipeSegment(file string, segment common.Location) error {

	var segmentId int
	err := duck.db.QueryRow(
		"SELECT id FROM segments WHERE file = ? AND pos_from = ? AND pos_to = ?",
		file, segment.From, segment.To,
	).Scan(segmentId)
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
	rows, err := duck.db.Query("SELECT id FROM segments WHERE file = ?", fileId)
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
		_, err = duck.db.Exec("DELETE FROM terms WHERE file = ?", file)
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

func (duck *DuckDB) GetAllMessages(path string) (iter.Seq[common.Message], error) {
	fileId, err := duck.getFileIdByPath(path)
	if err != nil {
		return nil, err
	}

	q := `
	SELECT
	    segments.id,
	    segments.pos_from,
	    segments.pos_to,
	    
	    messages.rel_from,
	    messages.rel_date_from,
	    messages.rel_date_to,
	    messages.date
	FROM messages
	JOIN segments on segments.id=messages.segment_id 
	WHERE segments.file_id = ?
	ORDER BY messages.date
	`
	rows, err := duck.db.Query(q, fileId)
	if err != nil {
		return nil, err
	}

	// Return an iterator over all messages in the given file path, ordered by date.
	// Messages are yielded one by one with correct To positions calculated from the next message.
	return func(yield func(common.Message) bool) {
		defer rows.Close()

		var (
			prevMessage                                                 *common.Message
			prevSegmentId, segmentId, segmentFrom, segmentTo, dateMicro int
		)
		for rows.Next() {
			cur := common.Message{}
			err = rows.Scan(
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
