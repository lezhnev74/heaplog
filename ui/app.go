package ui

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2"
	"github.com/marcboeker/go-duckdb"
	"golang.org/x/exp/mmap"
	"golang.org/x/xerrors"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/ingest"
	"heaplog_2024/query_language"
	"heaplog_2024/scanner"
	"heaplog_2024/search"
	"heaplog_2024/tokenizer"
)

// HeaplogApp is the main layer that manages use-cases connected to console/HTTP channel.
// It runs business logic packed in use-cases.
type HeaplogApp struct {
	db     *db.DbContainer
	search *search.Search
	cfg    Config
	ctx    context.Context
}

// DeleteQuery removes the query and its results
func (happ *HeaplogApp) DeleteQuery(queryId int) error {
	return happ.db.QueryDB.RemoveQuery(queryId)
}

// ListQueries returns all current queries (for the UI to render a list on the homepage)
func (happ *HeaplogApp) ListQueries() ([]db.Query, error) {
	return happ.db.QueryDB.List()
}

// Test	performs basic config test and tries to find a single message in a single file.
// If no error is found, it means that mostly all is set up correctly.
func (happ *HeaplogApp) Test() error {
	files, err := filepath.Glob(happ.cfg.FilesGlobPattern)
	if err != nil {
		return xerrors.Errorf("unable to find files at %s: %w", happ.cfg.FilesGlobPattern, err)
	}
	if len(files) == 0 {
		return xerrors.Errorf("unable to find files at %s: no files found", happ.cfg.FilesGlobPattern)
	}

	var file string
	file, err = filepath.Abs(files[0])
	if err != nil {
		return xerrors.Errorf("unable to find the file at %s: %w", file, err)
	}
	layouts, err := scanner.UgScan(file, happ.cfg.MessageStartRE, []common.Location{{From: 0, To: 10000}})
	if err != nil {
		return xerrors.Errorf("unable to test the file at %s: %w", file, err)
	}

	if len(layouts) == 0 {
		return xerrors.Errorf("no messages found in %s (check regular expression again)", file)
	}
	ml := layouts[0]

	// test date extraction:
	f, err := os.Open(file)
	if err != nil {
		return xerrors.Errorf("unable to test the file at %s: %w", file, err)
	}
	dateBuf := make([]byte, ml.DateTo-ml.DateFrom)
	_, err = f.ReadAt(dateBuf, int64(ml.DateFrom))
	if err != nil {
		return xerrors.Errorf("unable to test the file at %s: %w", file, err)
	}
	_, err = time.Parse(happ.cfg.DateFormat, string(dateBuf))
	if err != nil {
		return xerrors.Errorf("unable to test the file at %s: parse date: %w", file, err)
	}

	log.Printf("Great! Found a message in %s\n", file)
	return nil
}

// NewQuery start new search.
// If the same query already exists, it removes that and runs search again.
func (happ *HeaplogApp) NewQuery(text string, min *time.Time, max *time.Time) (newQuery db.Query, isFullscan bool, err error) {
	queries, err := happ.db.QueryDB.List()
	if err != nil {
		return
	}

	// Test and Remove existing query
	cmpTime := func(t1, t2 *time.Time) bool {
		var t1Int, t2Int int64
		if t1 != nil {
			t1Int = t1.UnixMicro()
		}
		if t2 != nil {
			t2Int = t2.UnixMicro()
		}
		return t1Int == t2Int
	}

	for _, q := range queries {
		if q.Text == text && cmpTime(q.Min, min) && cmpTime(q.Max, max) {
			// found the same query, so remove it
			go func() {
				_ = happ.db.RemoveQuery(q.Id)
			}()
		}
	}

	// Run a new query
	var queryExpr *query_language.Expression
	queryExpr, err = query_language.ParseUserQuery(text)
	if err != nil {
		err = xerrors.Errorf("parse query text: %w", err)
		return
	}

	var messagesIt go_iterators.Iterator[db.Message]
	messagesIt, isFullscan, err = happ.search.Search(
		queryExpr,
		min,
		max,
		happ.cfg.DateFormat,
		func(in []byte) [][]byte {
			return tokenizer.Tokenize(in, int(happ.cfg.MinTermLen), int(happ.cfg.MaxTermLen))
		},
		int(happ.cfg.Concurrency),
	)
	if err != nil {
		err = xerrors.Errorf("new query: %w", err)
		return
	}

	newQuery, err = happ.db.CheckinQuery(happ.ctx, text, min, max, messagesIt)

	return
}

func (happ *HeaplogApp) Page(queryId int, from, to *time.Time, page, pageSize, pageSkip int) (rows []string, err error) {
	messages, err := happ.db.QueryDB.Page(queryId, from, to, page, pageSize)
	if err != nil {
		err = xerrors.Errorf("page failed: %w", err)
		return
	}

	// Apply skip-offset:
	messages = messages[min(len(messages), pageSkip):]

	return happ.fetchMessages(queryId, messages)
}

func (happ *HeaplogApp) All(queryId int, from, to *time.Time) (rows go_iterators.Iterator[string], err error) {
	// stream from the query storage
	messages, err := happ.db.QueryDB.Stream(queryId, from, to)
	if err != nil {
		err = xerrors.Errorf("page failed: %w", err)
		return
	}

	// batch messages
	messageBatchesIt := go_iterators.NewBatchingIterator(messages, 1000)

	// map stream to message strings (read from heap files)
	retIterator := go_iterators.NewMappingIterator(messageBatchesIt, func(messageBatch []db.Message) []string {
		rowBatch, err := happ.fetchMessages(queryId, messageBatch)
		if err != nil {
			log.Fatal(err)
		}
		return rowBatch
	})

	// flatten
	flatRows := go_iterators.NewDynamicSliceIterator(
		func() ([]string, error) {
			return retIterator.Next()
		},
		func() error {
			return nil
		},
	)

	return flatRows, nil
}

// Query returns query description with sub-query support (time scope on the query)
func (happ *HeaplogApp) Query(queryId int, from, to *time.Time) (query db.Query, err error) {
	query, err = happ.db.QueryDB.FindQuery(queryId)
	if err != nil {
		return
	}
	if from != nil {
		query.Min = from
	}
	if to != nil {
		query.Max = to
	}
	query.Messages, err = happ.db.QueryDB.Count(queryId, from, to)
	return
}

func (happ *HeaplogApp) fetchMessages(queryId int, messages []db.Message) (rows []string, err error) {
	// Read actual messages from the source files
	var (
		file           string
		lastFileId     int
		lastFileReader *mmap.ReaderAt
	)
	for _, m := range messages {
		if lastFileId != m.FileId {
			lastFileId = m.FileId

			// Open a new file stream:
			file, err = happ.db.GetFile(m.FileId)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// this can happen if the target file was already removed, but cleanup was not performed yet
					log.Printf("query %d page: looks like the file[%d] is removed", queryId, m.FileId)
					continue
				}
				err = xerrors.Errorf("page failed: find file: %w", err)
				return
			}

			if lastFileReader != nil {
				_ = lastFileReader.Close()
			}
			lastFileReader, err = mmap.Open(file)
			if err != nil {
				err = xerrors.Errorf("page failed: mmap file: %w", err)
				return
			}
		}

		b := make([]byte, m.Loc.To)
		_, err = lastFileReader.ReadAt(b, int64(m.Loc.From))
		if err != nil {
			err = xerrors.Errorf("page failed: mmap read: %w", err)
			return
		}
		rows = append(rows, string(bytes.TrimRight(b, "\n")))
	}
	if lastFileReader != nil {
		_ = lastFileReader.Close()
	}

	return
}

func NewHeaplog(ctx context.Context, cfg Config, startBackground bool) (*HeaplogApp, error) {

	common.EnableLogging = cfg.EnableLogging

	// 1. Init the database
	connector, err := db.PrepareDuckDB(cfg.StoragePath, int(cfg.DuckdbMaxMemMb))
	if err != nil {
		return nil, err
	}

	_db := sql.OpenDB(connector)
	_db.SetConnMaxIdleTime(5 * time.Second)

	err = db.Migrate(_db)
	if err != nil {
		return nil, err
	}

	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	messageAppender, err := duckdb.NewAppenderFromConn(conn, "", "file_segments_messages")
	if err != nil {
		return nil, err
	}
	resultsAppender, err := duckdb.NewAppenderFromConn(conn, "", "query_results")
	if err != nil {
		return nil, err
	}

	dbContainer := &db.DbContainer{
		DB:         _db,
		FilesDb:    db.NewFilesDb(_db),
		SegmentsDb: db.NewSegmentsDb(_db),
		MessagesDb: db.NewMessagesDb(_db, messageAppender),
		QueryDB:    db.NewQueryDb(_db, resultsAppender),
	}

	// 2. Init Services
	tok := func(in []byte) [][]byte {
		return tokenizer.Tokenize(in, int(cfg.MinTermLen), int(cfg.MaxTermLen))
	}
	ii, err := inverted_index_2.NewInvertedIndex(cfg.StoragePath, cfg.EnableLogging)
	if err != nil {
		return nil, err
	}
	layoutFile := func(file string, locations []common.Location) ([]scanner.MessageLayout, error) {
		return scanner.UgScan(file, cfg.MessageStartRE, locations)
	}
	pd := func(b []byte) (time.Time, error) {
		return time.Parse(cfg.DateFormat, string(b))
	}
	segmentSize := uint64(5_000_000)
	ingestor := ingest.NewIngest(ctx, layoutFile, pd, tok, dbContainer, ii, segmentSize, int(cfg.Concurrency))
	_search := search.NewSearch(ctx, dbContainer, ii, cfg.DateFormat)

	_discover := ingest.NewDiscover([]string{cfg.FilesGlobPattern}, dbContainer.FilesDb)

	// 3. Start background procs
	if startBackground {
		// Clear up queries
		go func() {
			t := common.InstantTick(time.Minute)
			for range t {
				queries, err := dbContainer.List()
				if err != nil {
					log.Printf("cleanup queries: %s", err.Error())
					return
				}
				ttl := time.Hour * 24
				for _, q := range queries {
					if time.Since(*q.BuiltAt) > ttl {
						err = dbContainer.RemoveQuery(q.Id)
						if err != nil {
							log.Printf("cleanup queries: %s", err.Error())
							return
						}
					}
				}

				debug.FreeOSMemory()
			}
		}()
		// Ingest
		go func() {
			t := common.InstantTick(time.Minute * 10)
			for range t {
				_, obsoletes, err := _discover.DiscoverFiles()
				if err != nil {
					log.Printf("discovering files stopped: %s", err)
					return
				}
				if len(obsoletes) > 0 {
					err = db.ClearUp(dbContainer, ii)
					if err != nil {
						log.Printf("cleaning up: %s", err)
						return
					}
				}

				allFiles, err := dbContainer.AllFiles()
				if err != nil {
					log.Printf("unable to read files for ingesting: %s", err)
					return
				}

				err = ingestor.IndexConcurrent(allFiles, int(cfg.Concurrency))
				if err != nil {
					log.Printf("ingest: %s", err)
					return
				}

				// After each ingestion cycle, give up memory to OS
				common.CleanMem()
			}
		}()
		//Merge
		go func() {
			t := common.InstantTick(time.Minute * 10)
			for range t {
				for {
					merged, err := ii.Merge(30, 30, int(cfg.Concurrency))
					if err != nil {
						log.Printf("merging inverted index segments: %s", err)
					}
					if merged == 0 {
						break
					}
				}
			}
		}()
	}

	// 4. Let's run it, huh?
	return &HeaplogApp{
		db:     dbContainer,
		search: _search,
		cfg:    cfg,
		ctx:    ctx,
	}, nil
}
