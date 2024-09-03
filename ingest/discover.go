package ingest

import (
	"golang.org/x/xerrors"
	"heaplog_2024/db"
	"log"
	"path/filepath"
)

// Discover looks for files changes in the monitored locations.
// It checks in existing files and handles obsolete files.
// Duckdb is used as a storage for all discovered files and calculates obsolete/new ones.
type Discover struct {
	globs   []string // glob patterns for all files to index
	storage *db.FilesDb
}

func (d *Discover) DiscoverFiles() (news, obsoletes []string, err error) {
	allCurrentFiles := make([]string, 0, 20)
	for _, g := range d.globs {
		files := make([]string, 0)
		files, err = filepath.Glob(g)
		if err != nil {
			err = xerrors.Errorf("unable to discover files at %s: %w", g, err)
			return
		}
		for _, f := range files {
			f, err = filepath.Abs(f)
			if err != nil {
				log.Printf("unable to get absolute path to the discovered file %s: %s", f, err)
				continue
			}
			allCurrentFiles = append(allCurrentFiles, f)
		}
	}

	news, obsoletes, err = d.storage.CheckInFiles(allCurrentFiles)
	if err != nil {
		err = xerrors.Errorf("failed to check in discovered files: %w", err)
		return
	}

	if len(news) > 0 || len(obsoletes) > 0 {
		report := "files discovered:\n"
		for i, _ := range news {
			report += "+ " + news[i] + "\n"
		}
		for i, _ := range obsoletes {
			report += "- " + obsoletes[i] + "\n"
		}
		log.Printf(report)
	}

	return
}

func NewDiscover(globs []string, storage *db.FilesDb) *Discover {
	return &Discover{globs: globs, storage: storage}
}
