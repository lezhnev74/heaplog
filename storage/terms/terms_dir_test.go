package terms_test

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"heaplog/storage"
	"heaplog/storage/terms"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestMultiplePut(t *testing.T) {
	// Plan:
	// 1. Put terms twice: make 2 terms files
	// 2. Read all terms: combine reading from 2 files
	// 3. Assert all terms are returned, make sure ids are consistent

	// Exec:
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	// 1. Put terms twice: make 2 terms files
	terms1 := []string{"term1", "term2"}
	terms2 := []string{"term0", "term2", "term3"}

	termsDir, err := terms.NewTermsDir(s)
	require.NoError(t, err)

	err = termsDir.Put(terms1)
	require.NoError(t, err)

	err = termsDir.Put(terms2)
	require.NoError(t, err)

	// 2. Read all terms: combine reading from 2 files
	allTerms, err := termsDir.All()
	require.NoError(t, err)

	// 3. Assert all terms are returned, make sure ids are consistent
	expectedTerms := []string{
		"term0",
		"term1",
		"term2",
		"term3",
	}
	require.Equal(t, expectedTerms, allTerms)
}

func TestGetMatching(t *testing.T) {
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	termsDir, err := terms.NewTermsDir(s)
	require.NoError(t, err)

	err = termsDir.Put([]string{"abc", "bce"}) // 1, 2
	require.NoError(t, err)
	err = termsDir.Put([]string{"ce"}) // 3
	require.NoError(t, err)

	matchTerms, err := termsDir.GetMatchedTermIds(func(term string) bool {
		return strings.Contains(term, "ce")
	})
	require.NoError(t, err)
	require.Equal(t, []string{"bce", "ce"}, matchTerms)
}

func TestPutMerge(t *testing.T) {
	// Plan:
	// 1. Put terms twice: make 2 terms files
	// 2. Merge terms
	// 3. Read again and make sure results are predictable

	// Exec:
	path, s := prepareStorage(t)
	defer os.RemoveAll(path)
	defer s.Close()

	termsDir, err := terms.NewTermsDir(s)
	require.NoError(t, err)

	// 1. Put terms twice: make 2 terms files
	terms1 := []string{"term1", "term2"}
	terms2 := []string{"term0", "term2", "term3"}

	err = termsDir.Put(terms1)
	require.NoError(t, err)

	err = termsDir.Put(terms2)
	require.NoError(t, err)

	// 2. Merge terms
	err = termsDir.Merge()
	require.NoError(t, err)
	err = termsDir.Cleanup()
	require.NoError(t, err)

	// 3. Read again and make sure results are predictable
	alltermIds, err := termsDir.All()
	require.NoError(t, err)
	expectedTerms := []string{
		"term0",
		"term1",
		"term2",
		"term3",
	}
	require.Equal(t, expectedTerms, alltermIds)
}

func TestMergePerformance(t *testing.T) {

	// Here I am profiling how memory is used for big concurrent put/merge work.

	path, db := prepareStorage(t)
	defer os.RemoveAll(path)
	defer db.Close()

	termsDir, err := terms.NewTermsDir(db)
	require.NoError(t, err)

	ctxPut, stopPut := context.WithCancel(context.Background())
	ctxMerge, stopMerge := context.WithCancel(context.Background())

	go func() {
		for {
			time.Sleep(3 * time.Second)
			storage.PrintStats(db)

			var fstCount, fstLen int64
			r := db.QueryRow(`SELECT count(*),sum(octet_length(fst)) FROM fst`)
			r.Scan(&fstCount, &fstLen)
			log.Printf("FSTs: %05d/%010d bytes", fstCount, fstLen)
		}
	}()

	// 1. Load multiple Put operations
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				select {
				case <-ctxPut.Done():
					return
				default:
					randomTerms := make([]string, 0, 100_000)
					for j := 0; j < cap(randomTerms); j++ {
						randomTerms = append(randomTerms, generateRandomString(20))
					}
					require.NoError(t, termsDir.Put(randomTerms))
				}
			}
		}()
	}

	// 2. Merge terms
	go func() {
		for {
			select {
			case <-ctxMerge.Done():
				return
			default:
				err = termsDir.Merge()
				require.NoError(t, err)
				err = termsDir.Cleanup()
				require.NoError(t, err)
			}
		}
	}()

	time.Sleep(time.Second * 5)
	stopPut()
	time.Sleep(time.Second * 10)
	stopMerge()
	time.Sleep(time.Second * 10)
	runtime.GC()
	time.Sleep(time.Second * 5)

	// measure memory consumption
	//f, err := os.Create(fmt.Sprintf("./profile_%d.tmp", time.Now().Unix()))
	//if err != nil {
	//	log.Fatal("could not create Mem profile: ", err)
	//}
	//p := pprof.Lookup("heap")
	//require.NoError(t, p.WriteTo(f, 0))
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)

	for i := range result {
		result[i] = charset[random.Intn(len(charset))]
	}
	return string(result)
}

func prepareStorage(t *testing.T) (path string, db *sql.DB) {

	storagePath, _ := os.MkdirTemp("", "")
	connector, err := storage.PrepareDuckDB(storagePath, 450)
	require.NoError(t, err)

	db = sql.OpenDB(connector)
	require.NoError(t, err)

	require.NoError(t, storage.Migrate(db))

	return storagePath, db
}
