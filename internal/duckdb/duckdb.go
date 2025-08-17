package duckdb

import (
	"context"
	"database/sql"
	"embed"
	"io"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"

	"heaplog_2024/internal/common"
)

//go:embed migrations
var migrationFS embed.FS

type DuckDB struct {
	db *sql.DB
}

func (duck *DuckDB) Open(ctx context.Context, dir string) (err error) {
	duck.db, err = sql.Open("duckdb", dir)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		duck.Close() // Flush buffers
	}()
	return nil
}

func (duck *DuckDB) Close() (err error) { return duck.db.Close() }

func (duck *DuckDB) getFileIdByPath(path string) (id int, err error) {
	err = duck.db.QueryRow("SELECT id FROM files WHERE path = ?", path).Scan(&id)
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

func (duck *DuckDB) putSegment(file string, terms [][]byte, messages []common.Message) error {
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
		return err
	}
	segmentId := 0
	err = duck.db.QueryRow(`SELECT nextval('segment_ids')`).Scan(&segmentId)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		"INSERT INTO segments (id, file_id, pos_from, pos_to, date_min, date_max) VALUES (?, ?, ?, ?)",
		segmentId,
		fileId,
		messages[0].Loc.From,
		messages[len(messages)-1].Loc.To,
		messages[0].Date,
		messages[len(messages)-1].Date,
	)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		_, err = tx.Exec(
			"INSERT INTO messages (segment_id, rel_from, rel_to, rel_date_from, rel_date_to, date) VALUES (?, ?, ?, ?, ?, ?)",
			segmentId,
			msg.Loc.From-messages[0].Loc.From,
			msg.Loc.To-messages[0].Loc.From,
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
	return duck.wipeSegments(file)
}
