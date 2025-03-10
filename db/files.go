package db

import (
	"database/sql"
	"fmt"
	"heaplog_2024/common"
	"slices"
	"strings"
	"sync"

	"golang.org/x/xerrors"
)

type FilesDb struct {
	db *sql.DB
	m  sync.Mutex
}

func NewFilesDb(db *sql.DB) *FilesDb {
	return &FilesDb{
		db: db,
	}
}

func (fdb *FilesDb) ReserveFileId() (fileId uint32, err error) {
	r := fdb.db.QueryRow(`SELECT nextval('file_ids');`)
	err = r.Scan(&fileId)
	if err != nil {
		err = xerrors.Errorf("unable to check in a query: %w", err)
	}
	return
}

// CheckInFiles replaces tracked files with the given.
// It returns obsolete files so the client can do other clean-ups like inverted index removal.
func (fdb *FilesDb) CheckInFiles(actualFiles []string) (newFiles, obsoleteFiles []string, err error) {
	// concurrent writes for the same row in a table will fail,
	// so we need to synchronize writing access to this table

	fdb.m.Lock()
	defer fdb.m.Unlock()

	// 1. Calculate new and obsolete actualFiles
	knownFiles, err := fdb.AllFiles()
	if err != nil {
		return
	}
	for _, f := range actualFiles {
		if !slices.Contains(knownFiles, f) {
			newFiles = append(newFiles, f)
		}
	}
	for _, f := range knownFiles {
		if !slices.Contains(actualFiles, f) {
			obsoleteFiles = append(obsoleteFiles, f)
		}
	}

	// 2. Apply DB change

	// 2.1 Insert New
	st, err := fdb.db.Prepare("INSERT INTO files(id,path) VALUES(nextval('file_ids'),?)")
	if err != nil {
		err = xerrors.Errorf("insert file: %w", err)
		return
	}
	defer st.Close()
	for _, newFile := range newFiles {
		_, err = st.Exec(newFile)
		if err != nil {
			err = xerrors.Errorf("insert file: %w", err)
			return
		}
	}

	// 2.2 Delete Obsolete
	if len(obsoleteFiles) == 0 {
		return
	}

	whereExpr := strings.TrimRight(strings.Repeat("?,", len(obsoleteFiles)), ",")
	_, err = fdb.db.Exec(
		fmt.Sprintf("DELETE FROM files WHERE path IN (%s)", whereExpr),
		common.SliceToAny(obsoleteFiles)...,
	)
	if err != nil {
		err = xerrors.Errorf("unable to delete obsolete files: %w", err)
	}
	return
}

func (fdb *FilesDb) GetFileId(file string) (id int, err error) {
	r := fdb.db.QueryRow("SELECT Id FROM files WHERE path=?", file)
	err = r.Scan(&id)
	if err != nil {
		err = fmt.Errorf("getFile %s: %w", file, err)
	}
	return
}

func (fdb *FilesDb) GetFile(fileId int) (file string, err error) {
	r := fdb.db.QueryRow("SELECT path FROM files WHERE Id=?", fileId)
	err = r.Scan(&file)
	if err != nil {
		err = fmt.Errorf("getFile %d: %w", fileId, err)
	}
	return
}

func (fdb *FilesDb) AllFiles() (files []string, err error) {

	var (
		path string
	)
	files = make([]string, 0)

	selectResults, err := fdb.db.Query("SELECT path FROM files")
	if err != nil {
		return nil, err
	}
	defer selectResults.Close()

	for selectResults.Next() {
		err = selectResults.Scan(&path)
		if err != nil {
			return nil, err
		}

		files = append(files, path)
	}

	return
}
