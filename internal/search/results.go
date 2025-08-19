package search

import (
	"iter"
	"time"

	"heaplog_2024/internal/common"
)

type SearchResult struct {
	Id       int
	Query    string
	Date     time.Time
	Messages int
	Finished bool
}

type ResultsStorage interface {
	// PutResultsAsync streams results into the storage in a separate goroutine, returns instantly.
	PutResultsAsync(query string, results iter.Seq[common.FileMessage]) (SearchResult, error)
	GetResultMessages(resultId int) (iter.Seq[common.FileMessage], error)
	GetResults(resultId int) (SearchResult, error)
	GetAllResults() ([]SearchResult, error)
	WipeResults(resultId int) error
}
