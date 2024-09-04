package test_util_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2"
	"github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"
	"heaplog_2024/common"
	"heaplog_2024/db"
	"heaplog_2024/ingest"
	"heaplog_2024/query_language"
	"heaplog_2024/scanner"
	"heaplog_2024/search"
	"heaplog_2024/test_util"
	"heaplog_2024/tokenizer"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"testing"
	"time"
)

var (
	messageStartPattern = `^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})\]`
	timeFormat          = "2006-01-02T15:04:05.000000-07:00"
)

func _TestSearch(t *testing.T) {
	t0 := time.Now()
	storageRoot := "/home/dmitry/Code/go/src/heaplog_2024/_local/s2"

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

	s, _, _, _, tok := buildDependencies(t, 5_000_000, storageRoot)

	query := `454533`
	expr, err := query_language.ParseUserQuery(query)
	require.NoError(t, err)

	messagesIt, isFullScan, err := s.Search(expr, nil, nil, timeFormat, tok, runtime.NumCPU())
	require.NoError(t, err)
	log.Printf("Full_scan: %t\n", isFullScan)

	c := 0
	for {
		_, err := messagesIt.Next()

		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		require.NoError(t, err)
		c++

		if c == 1 || c == 100 {
			log.Printf("%d results ready in %s\n", c, time.Now().Sub(t0).String())
		}
	}
	log.Printf("Found messages: %d\n", c)
	debug.FreeOSMemory()
	test_util.ProcStat()
}

func _TestIngest(t *testing.T) {
	storageRoot := "/home/dmitry/Code/go/src/heaplog_2024/_local/s4"

	//stop := traceRun(path.Join(storageRoot, "trace.out"))
	//defer stop()
	go func() {
		t := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-t.C:
				test_util.ProcStat()
				debug.FreeOSMemory()
			}
		}
	}()

	debug.SetGCPercent(10)
	_, ing, ii, fdb, _ := buildDependencies(t, 5_000_000, storageRoot)

	files := []string{
		"/home/dmitry/Code/go/src/heaplog_2024/_local/1kb.log",
		"/home/dmitry/Code/go/src/heaplog_2024/_local/10kb.log",
		"/home/dmitry/Code/go/src/heaplog_2024/_local/1gb.log",
		"/home/dmitry/Code/go/src/heaplog_2024/_local/4gb_.log",
		"/home/dmitry/Code/go/src/heaplog_2024/_local/sample.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-10.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-11.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-12.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-13.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-14.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-15.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-16.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-17.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-18.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-19.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-20.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-21.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-22.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-23.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-24.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-25.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-26.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-27.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-28.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-29.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-30.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-07-31.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-01.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-02.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-03.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-04.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-05.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-06.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-07.log",
		//"/home/dmitry/Code/go/src/heaplog_2024/_local/logs/laravel-2024-08-08.log",
	}
	_, _, err := fdb.CheckInFiles(files)
	require.NoError(t, err)

	log.Printf("Go ingest")

	err = ing.IndexConcurrent(files, runtime.NumCPU())
	require.NoError(t, err)

	runtime.GC()
	debug.FreeOSMemory()
	test_util.ProcStat()

	// MERGE II:
	fmt.Printf("MERGE START\n")
	//return
	for {
		mergedSegments, err := ii.Merge(30, 1000, runtime.NumCPU())
		require.NoError(t, err)
		if mergedSegments == 0 {
			break
		}
	}
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

	dbContainer := &db.DbContainer{
		DB:         _db,
		FilesDb:    db.NewFilesDb(_db),
		SegmentsDb: db.NewSegmentsDb(_db),
		MessagesDb: db.NewMessagesDb(_db, appender),
	}

	tok := func(in []byte) [][]byte {
		return tokenizer.Tokenize(in, 4, 8)
	}
	ii, err := inverted_index_2.NewInvertedIndex(storageRoot)
	require.NoError(t, err)

	s := func(file string, locations []common.Location) ([]scanner.MessageLayout, error) {
		it, err := scanner.UgScanLocations(file, locations, messageStartPattern)
		require.NoError(t, err)
		return go_iterators.ToSlice(it), nil
	}
	pd := func(b []byte) (time.Time, error) {
		return time.Parse(timeFormat, string(b))
	}

	ing := ingest.NewIngest(s, pd, tok, dbContainer, ii, segmentSize, 1)

	_search := search.NewSearch(dbContainer, ii, timeFormat)

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
