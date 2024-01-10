package ingest

import (
	"golang.org/x/xerrors"
	"heaplog/storage"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Discover looks for files changes in the monitored locations.
// It checks in existing files and handles obsolete files.
// Duckdb is used as a storage for all discovered files and calculates obsolete/new ones.
type Discover struct {
	globs   []string // glob patterns for all files to index
	storage *storage.Storage
}

func (d *Discover) DiscoverFiles() error {
	allCurrentFiles := make(map[string]int64)
	for _, g := range d.globs {
		files, err := filepath.Glob(g)
		if err != nil {
			return xerrors.Errorf("unable to discover files at %s: %w", g, err)
		}
		for _, f := range files {
			finfo, err := os.Stat(f)
			if err != nil {
				log.Printf("unable to stat discovered file %s: %s", f, err)
				continue
			}
			f, err = filepath.Abs(f)
			if err != nil {
				log.Printf("unable to get absolute path to the discovered file %s: %s", f, err)
				continue
			}
			allCurrentFiles[f] = finfo.Size()
		}
	}

	obsoletes, news, err := d.storage.CheckInFiles(allCurrentFiles)
	if err != nil {
		return xerrors.Errorf("failed to check in discovered files: %w", err)
	}

	if len(news) > 0 || len(obsoletes) > 0 {
		for i, _ := range obsoletes {
			obsoletes[i] = "- " + obsoletes[i]
		}
		for i, _ := range news {
			news[i] = "+ " + news[i]
		}
		log.Printf("files discovered:\n%s\n%s", strings.Join(news, "\n"), strings.Join(obsoletes, "\n"))
	}

	return nil
}

func NewDiscover(globs []string, storage *storage.Storage) *Discover {
	return &Discover{globs: globs, storage: storage}
}
