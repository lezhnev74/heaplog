package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/lezhnev74/inverted_index_2"
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
func ClearUp(db *DbContainer, ii *inverted_index_2.InvertedIndex) error {

	// read ids as strings (helper)
	strIds := func(sql string) (ids []string, uintIds []uint32, err error) {
		r, err := db.Query(sql)
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
			uintIds = append(uintIds, uint32(fid))
		}
		return
	}

	// 1. See which files exist
	fileIds, _, err := strIds("SELECT id FROM files")
	if err != nil {
		return err
	}
	if len(fileIds) == 0 {
		// edge-case: all files are gone
		fileIds = append(fileIds, "9999999999999999999")
	}
	fileIdsString := strings.Join(fileIds, ",")

	// 2. Clean up segments
	danglingSegmentIds, segmentU32Ids, err := strIds(fmt.Sprintf("SELECT id FROM file_segments WHERE fileId NOT IN (%s)", fileIdsString))
	if err != nil {
		return err
	}

	if len(danglingSegmentIds) > 0 {
		segmentIdsString := strings.Join(danglingSegmentIds, ",")
		_, err = db.Exec(fmt.Sprintf("DELETE FROM file_segments_messages WHERE segmentId IN (%s)", segmentIdsString))
		if err != nil {
			return err
		}
	}

	_, err = db.Exec(fmt.Sprintf("DELETE FROM file_segments WHERE fileId NOT IN (%s)", fileIdsString))
	if err != nil {
		return err
	}

	err = ii.PutRemoved(segmentU32Ids)
	if err != nil {
		return err
	}

	// 3. Clear up query results
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
