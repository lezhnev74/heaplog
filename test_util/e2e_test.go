//nolint:unused
package test_util_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"testing"
	"time"

	"github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"

	"github.com/lezhnev74/inverted_index_2"

	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/ingest"
	"heaplog_2024/query_language"
	"heaplog_2024/scanner"
	"heaplog_2024/search"
	"heaplog_2024/test_util"
	"heaplog_2024/tokenizer"
)

var (
	messageStartPattern = `^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})\]`
	timeFormat          = "2006-01-02T15:04:05.000000-07:00"
)

func _TestSearch(t *testing.T) {
	//t0 := time.Now()
	storageRoot := "/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/local_test/storage"

	//go func() {
	//	t := time.NewTicker(time.Second)
	//	for {
	//		select {
	//		case <-t.C:
	//			debug.FreeOSMemory()
	//			test_util.ProcStat()
	//		}
	//	}
	//}()

	//stop := traceRun(path.Join(storageRoot, "trace.out"))
	//defer stop()

	s, _, _, dbc, tok := buildDependencies(t, 5_000_000, storageRoot)

	query := `error`
	expr, err := query_language.ParseUserQuery(query)
	require.NoError(t, err)

	messagesIt, isFullScan, err := s.Search(expr, nil, nil, timeFormat, tok, runtime.NumCPU())
	require.NoError(t, err)
	common.Out("Full_scan: %t\n", isFullScan)

	q, err := dbc.CheckinQuery(context.Background(), query, nil, nil, messagesIt)
	require.NoError(t, err)

	for !q.Finished {
		q, err = dbc.FindQuery(q.Id)
		require.NoError(t, err)
		fmt.Printf("query results: %d\n", q.Messages)
		time.Sleep(time.Second)
	}

	dbc.QueryDB.Flush()

	//c := 0
	//for range messagesIt {
	//
	//	require.NoError(t, err)
	//	c++
	//
	//	if c == 1 || c == 100 {
	//		common.Out("%d results ready in %s\n", c, time.Since(t0).String())
	//	}
	//}
	//common.Out("Found messages: %d\n", c)
	//debug.FreeOSMemory()
	//test_util.ProcStat()
}

func _TestIngest(t *testing.T) {
	storageRoot := "/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/test_storage"

	//stop := traceRun(path.Join(storageRoot, "trace.out"))
	//defer stop()

	go func() {
		t := time.NewTicker(5 * time.Second)
		for range t.C {
			test_util.ProcStat()
			debug.FreeOSMemory()
		}
	}()

	debug.SetGCPercent(10)
	_, ing, _, fdb, _ := buildDependencies(t, 5_000_000, storageRoot)

	files := []string{
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/1kb.log",
		//"/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/10kb.log",
		"/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/100Mb.log",
		//"/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/1gb.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/4gb_.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/sample.log",
		//"/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/logs/laravel-2025-02-20.log",
		//"/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/logs/laravel-2025-02-21.log",
	}
	_, _, err := fdb.CheckInFiles(files)
	require.NoError(t, err)

	common.Out("Go ingest")

	err = ing.Index(files)
	require.NoError(t, err)

	test_util.ProcStat()

	// MERGE II:
	//fmt.Printf("MERGE START\n")
	////return
	//for {
	//	mergedSegments, err := ii.Merge(30, 1000, runtime.NumCPU())
	//	require.NoError(t, err)
	//	if mergedSegments == 0 {
	//		break
	//	}
	//}
}

func buildDependencies(t *testing.T, segmentSize uint64, storageRoot string) (
	*search.Search,
	*ingest.Ingest,
	*inverted_index_2.InvertedIndex,
	*db.DbContainer,
	func(in []byte) [][]byte,
) {
	connector, err := db.PrepareDuckDB(storageRoot, 100)
	require.NoError(t, err)
	_db := sql.OpenDB(connector)
	err = db.Migrate(_db)
	require.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	appender, err := duckdb.NewAppenderFromConn(conn, "", "file_segments_messages")
	require.NoError(t, err)
	resultsAppender, err := duckdb.NewAppenderFromConn(conn, "", "query_results")
	require.NoError(t, err)

	dbContainer := &db.DbContainer{
		DB:         _db,
		FilesDb:    db.NewFilesDb(_db),
		SegmentsDb: db.NewSegmentsDb(_db),
		MessagesDb: db.NewMessagesDb(_db, appender),
		QueryDB:    db.NewQueryDb(_db, resultsAppender),
	}

	tok := func(in []byte) [][]byte {
		return tokenizer.Tokenize(in, 4, 8)
	}
	ii, err := inverted_index_2.NewInvertedIndex(storageRoot, true)
	require.NoError(t, err)

	s := func(file string, locations []common.Location) ([]scanner.MessageLayout, error) {
		//layouts, err := scanner.UgScanLocations(file, locations, messageStartPattern)
		layouts, err := scanner.UgScan(file, messageStartPattern, locations)
		require.NoError(t, err)
		return layouts, nil
	}
	pd := func(b []byte) (time.Time, error) {
		return time.Parse(timeFormat, string(b))
	}

	ing := ingest.NewIngest(context.Background(), s, pd, tok, dbContainer, ii, segmentSize, 1)

	_search := search.NewSearch(context.Background(), dbContainer, ii, timeFormat)

	return _search, ing, ii, dbContainer, tok
}

func traceRun(toFile string) func() error {
	f, err := os.Create(toFile)
	if err != nil {
		log.Fatalf("failed to create trace output file: %v", err)
	}

	if err := trace.Start(f); err != nil {
		log.Fatalf("failed to start trace: %v", err)
	}

	return func() error {
		trace.Stop()
		return f.Close()
	}
}
