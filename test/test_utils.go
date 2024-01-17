package test

import (
	"database/sql"
	"embed"
	"fmt"
	"github.com/stretchr/testify/require"
	"heaplog/common"
	"heaplog/indexer"
	"heaplog/scanner"
	"heaplog/storage"
	"heaplog/tokenizer"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"runtime/pprof"
	"testing"
	"time"
)

// This package contains utilities used in tests only
// this seems to be the only sane way to share code during tests.

var (
	// these are default formats used in my tests:
	messageStartPattern = regexp.MustCompile(`(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`)
	dateLayout          = "2006-01-02 15:04:05.000000"
	tokenizerFunc       = func(input string) []string {
		return tokenizer.Tokenize(input, 4, 40)
	}
	unboundTokenizerFunc = func(input string) []string {
		return tokenizer.Tokenize(input, 1, 40)
	}
	_scanner = scanner.NewScanner(dateLayout, messageStartPattern, 100, 1000)
	_indexer = indexer.NewIndexer(_scanner, tokenizerFunc)

	//go:embed _testdata
	testdataFS embed.FS
)

func MakeTimeV(value string) time.Time {
	t, err := time.ParseInLocation(dateLayout, value, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func MakeTimeP(value string) *time.Time {
	t := MakeTimeV(value)
	return &t
}

func PrepareTokenizers() (func(input string) []string, func(input string) []string) {
	return tokenizerFunc, unboundTokenizerFunc
}

func PrepareScanner() *scanner.Scanner { return _scanner }

func PrepareServices(t *testing.T, segmentSize int64) (
	s *storage.Storage,
	indexer *indexer.Indexer,
	storageRoot string,
) {
	storageRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	indexer = _indexer

	// DuckDB:
	s, err = storage.NewStorage(storageRoot, time.Millisecond, time.Millisecond)
	require.NoError(t, err)

	return
}

func PrepareDataSourceFiles(t *testing.T) (logFiles map[string]int64) {
	logFiles = make(map[string]int64)

	// Target dir:
	filesRoot, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	// Copy embedded files to the target dir:
	srcFiles := []string{"_testdata/file1.log", "_testdata/file2.log"}
	for _, srcFile := range srcFiles {
		_, filename := path.Split(srcFile)
		dstFile := path.Join(filesRoot, filename)

		srcF, err := testdataFS.Open(srcFile)
		require.NoError(t, err)

		dstF, err := os.Create(dstFile)
		require.NoError(t, err)

		fileLen, err := io.Copy(dstF, srcF)
		require.NoError(t, err)

		logFiles[dstFile] = fileLen
	}

	return
}

func DumpTable(db *sql.DB, table string, arguments int) {
	fmt.Printf("table %s\n", table)
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s`, table))
	if err != nil {
		panic(err)
	}
	vars := make([]any, arguments)
	row := 1
	for rows.Next() {
		switch arguments {
		case 1:
			err = rows.Scan(&vars[0])
			if err != nil {
				panic(err)
			}
		case 2:
			err = rows.Scan(&vars[0], &vars[1])
			if err != nil {
				panic(err)
			}
		case 3:
			err = rows.Scan(&vars[0], &vars[1], &vars[2])
			if err != nil {
				panic(err)
			}
		case 4:
			err = rows.Scan(&vars[0], &vars[1], &vars[2], &vars[3])
			if err != nil {
				panic(err)
			}
		case 5:
			err = rows.Scan(&vars[0], &vars[1], &vars[2], &vars[3], &vars[4])
			if err != nil {
				panic(err)
			}
		case 6:
			err = rows.Scan(&vars[0], &vars[1], &vars[2], &vars[3], &vars[4], &vars[5])
			if err != nil {
				panic(err)
			}
		}

		fmt.Printf("%s %d: %v\n", table, row, vars)
		row++
	}
}

func Profile(fn func()) {
	tt := time.Now()
	f, err := os.Create(fmt.Sprintf("./profile_%d.tmp", time.Now().Unix()))
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	fn()

	log.Printf("profiled in %s", time.Now().Sub(tt).String())
	pprof.StopCPUProfile()
}

// For comparison we need not account for Ids
func RemoveMessageIds(segments []common.IndexedSegment) {
	for i := range segments {
		for j := range segments[i].Messages {
			segments[i].Messages[j].Id = -1
		}
	}
}
