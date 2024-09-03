package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"golang.org/x/xerrors"
	"log"
	"path/filepath"
	"strconv"
	"strings"
)

var ErrNoData error = xerrors.Errorf("no data available")

type DbContainer struct {
	*FilesDb
	*MessagesDb
	*SegmentsDb
	*QueryDB
	*sql.DB
}

// ClearUp removes all data associated with removed files
func ClearUp(db *DbContainer) error {

	// read ids as strings (helper)
	strIds := func(sql string) (ids []string, err error) {
		r, err := db.Query("SELECT id FROM files")
		if err != nil {
			return
		}
		for r.Next() {
			fid := 0
			err = r.Scan(&fid)
			if err != nil {
				return
			}
			ids = append(ids, strconv.Itoa(fid))
		}
		return
	}

	// 1. See which files exist
	fileIds, err := strIds("SELECT id FROM files")
	if err != nil {
		return err
	}
	fileIdsString := strings.Join(fileIds, ",")

	// 2. Clean up segments
	segmentIds, err := strIds(fmt.Sprintf("SELECT id FROM file_segments WHERE fileId NOT IN (%s)", fileIdsString))
	if err != nil {
		return err
	}
	segmentIdsString := strings.Join(segmentIds, ",")

	_, err = db.Exec(fmt.Sprintf("DELETE FROM file_segments WHERE fileId NOT IN (%s)", fileIdsString))
	if err != nil {
		return err
	}

	// 3. Clear up messages
	_, err = db.Exec(fmt.Sprintf("DELETE FROM file_segments_messages WHERE segmentId NOT IN (%s)", segmentIdsString))
	if err != nil {
		return err
	}

	// 4. Clear up query results
	_, err = db.Exec(fmt.Sprintf("DELETE FROM query_results WHERE fileId NOT IN (%s)", fileIdsString))
	if err != nil {
		return err
	}

	return nil
}

func OpenDb(storageDir string, duckdbMemLimitMb int) (*sql.DB, error) {
	connector, err := PrepareDuckDB(storageDir, duckdbMemLimitMb)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)

	err = Migrate(db)

	return db, err
}

func PrepareDuckDB(storageDir string, duckdbMemLimitMb int) (driver.Connector, error) {
	duckFile := filepath.Join(storageDir, "db")

	// add config values
	if duckdbMemLimitMb < 100 {
		log.Fatalf("Duckdb mem limit is too low: %d", duckdbMemLimitMb)
	}
	duckdbMemLimit := fmt.Sprintf("%dMb", duckdbMemLimitMb)
	duckOptions := map[string]string{
		"memory_limit":               duckdbMemLimit,
		"temp_directory":             storageDir,
		"immediate_transaction_mode": "true",
	}
	duckFile += "?"
	for k, v := range duckOptions {
		duckFile += fmt.Sprintf("%s=%s&", k, v)
	}

	connector, err := duckdb.NewConnector(duckFile, nil)

	return connector, err
}
