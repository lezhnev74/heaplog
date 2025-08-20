package ui

import (
	"context"
	"errors"
	"log"
	"regexp"
	"time"

	"github.com/lezhnev74/inverted_index_2"

	"heaplog_2024/internal"
	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ingest"
	"heaplog_2024/internal/persistence"
	"heaplog_2024/internal/search"
)

type Heaplog struct {
	Ingestor *ingest.Ingestor
	Searcher *search.Search
}

func NewHeaplog(ctx context.Context) Heaplog {
	logger, err := internal.NewLogger("prod")
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	cfg, err := LoadConfig()
	if err != nil && errors.Is(err, errNoConfigFile) {
		logger.Info("No config file found, using default config")
	} else if err != nil {
		log.Fatal(err)
	}

	duck, err := persistence.NewDuckDB(ctx, cfg.StoragePath, logger)
	err = duck.Migrate()
	if err != nil {
		log.Fatal(err)
	}

	ii, err := inverted_index_2.NewInvertedIndex(cfg.StoragePath, false)
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
		1_000_000,
		1,
		persistentIndex,
		logger,
		indexer,
	)

	searcher := search.NewSearch(ctx, tokenize, persistentIndex, logger)

	return Heaplog{Ingestor: ingestor, Searcher: searcher}
}
