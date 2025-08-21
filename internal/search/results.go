package search

import (
	"iter"

	"heaplog_2024/internal/common"
)

type ResultsStorage interface {
	// PutResultsAsync streams results into the storage in a separate goroutine, returns instantly.
	PutResultsAsync(q common.UserQuery, results iter.Seq[common.FileMessage]) (
		common.SearchResult,
		chan struct{}, // done chan
		error,
	)
	GetResultMessages(resultId, skip, limit int) (iter.Seq[common.FileMessage], error)
	// GetResults returns the result with given ids or all if empty.
	GetResults(resultIds []int) (map[int]*common.SearchResult, error)
	WipeResults(resultId int) error
}
