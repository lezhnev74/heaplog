package ui

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/lezhnev74/inverted_index_2"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ingest"
	"heaplog_2024/internal/persistence"
	"heaplog_2024/internal/search"
)

type Heaplog struct {
	Logger   *zap.Logger
	Ingestor *ingest.Ingestor
	Searcher *search.Search
	Results  search.ResultsStorage
	II       *inverted_index_2.InvertedIndex
}

// TestConfig	performs basic config test and tries to find a single message in a single file.
// If no error is found, it means that mostly all is set up correctly.
func TestConfig(cfg Config) (string, error) {
	files, err := filepath.Glob(cfg.FilesGlobPattern)
	if err != nil {
		return "", fmt.Errorf("unable to find files at %s: %w", cfg.FilesGlobPattern, err)
	}
	if len(files) == 0 {
		return "", fmt.Errorf("unable to find files at %s: no files found", cfg.FilesGlobPattern)
	}

	var file string
	file, err = filepath.Abs(files[0])
	if err != nil {
		return "", fmt.Errorf("unable to find the file at %s: %w", file, err)
	}

	fileInfo, err := os.Stat(file)
	if err != nil {
		return "", fmt.Errorf("unable to get file info at %s: %w", file, err)
	}
	fileSize := fileInfo.Size()

	scannedMessages, err := ingest.Scan(
		file,
		int(fileSize),
		cfg.MessageStartRE,
		[]common.Location{{From: 0, To: 100_000}},
	)
	if err != nil {
		return "", fmt.Errorf("unable to test the file at %s: %w", file, err)
	}

	layouts := slices.Collect(scannedMessages)

	if len(layouts) == 0 {
		return "", fmt.Errorf("no messages found in %s (check regular expression again)", file)
	}
	ml := layouts[0]

	// test date extraction:
	f, err := os.Open(file)
	if err != nil {
		return "", fmt.Errorf("unable to test the file at %s: %w", file, err)
	}
	dateBuf := make([]byte, ml.DateLoc.To-ml.DateLoc.From)
	_, err = f.ReadAt(dateBuf, int64(ml.DateLoc.From))
	if err != nil {
		return "", fmt.Errorf("unable to test the file at %s: %w", file, err)
	}
	_, err = time.Parse(cfg.DateFormat, string(dateBuf))
	if err != nil {
		return "", fmt.Errorf("unable to test the file at %s: parse date: %w", file, err)
	}

	return file, nil
}

func NewHeaplog(ctx context.Context, logger *zap.Logger, cfg Config) Heaplog {

	dbFile := path.Join(cfg.StoragePath, "heaplog.db")
	duck, err := persistence.NewDuckDB(ctx, dbFile, logger)
	if err != nil {
		log.Fatal(err)
	}

	iiPath := path.Join(cfg.StoragePath, "ii")
	err = os.MkdirAll(iiPath, 0755)
	if err != nil {
		log.Fatal(err)
	}

	ii, err := inverted_index_2.NewInvertedIndex(iiPath, false)
	if err != nil {
		log.Fatal(err)
	}

	persistentIndex, err := persistence.NewPersistentIndex(duck, ii)
	if err != nil {
		log.Fatal(err)
	}

	tokenize := func(b []byte) [][]byte { return common.Tokenize(b, cfg.MinTermLen, cfg.MaxTermLen) }
	indexer := ingest.NewIndexer(
		ctx,
		logger,
		tokenize,
		func(b []byte) (time.Time, error) { return time.Parse(cfg.DateFormat, string(b)) },
	)

	ingestor := ingest.NewIngestor(
		[]string{cfg.FilesGlobPattern},
		regexp.MustCompile(cfg.MessageStartRE),
		5_000_000,
		cfg.Concurrency,
		persistentIndex,
		logger,
		indexer,
	)

	searcher := search.NewSearch(ctx, tokenize, persistentIndex, logger)

	return Heaplog{
		Logger:   logger,
		Ingestor: ingestor,
		Searcher: searcher,
		Results:  duck,
		II:       ii,
	}
}
