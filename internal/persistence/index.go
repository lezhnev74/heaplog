package persistence

import (
	"fmt"

	"github.com/lezhnev74/inverted_index_2"

	"heaplog_2024/internal/common"
)

// Index combines relations db and inverted index
type Index struct {
	*DuckDB
	ii *inverted_index_2.InvertedIndex
}

func NewPersistentIndex(duck *DuckDB, ii *inverted_index_2.InvertedIndex) (*Index, error) {
	return &Index{
		DuckDB: duck,
		ii:     ii,
	}, nil
}

func (i Index) GetRelevantSegments(terms [][]byte) (map[string][]int, error) {
	r, err := i.ii.PrefixSearch(terms)
	if err != nil {
		return nil, fmt.Errorf("inverted index lookup: %w", err)
	}
	segmentIds := make(map[string][]int, len(terms))
	for term, ids := range r {
		for _, id := range ids {
			segmentIds[term] = append(segmentIds[term], int(id))
		}
	}
	return segmentIds, nil
}

func (i Index) PutSegment(file string, terms [][]byte, messages []common.Message) (int, error) {
	segmentId, err := i.DuckDB.PutSegment(file, messages)
	if err != nil {
		return segmentId, fmt.Errorf("db put segment: %w", err)
	}

	err = i.ii.Put(terms, uint32(segmentId))
	if err != nil {
		i.DuckDB.WipeSegment(file, messages[0].Loc)
		return segmentId, fmt.Errorf("inverted index put terms: %w", err)
	}

	return segmentId, nil
}
