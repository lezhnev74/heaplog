package persistence

import (
	"context"
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

func (i Index) GetRelevantSegments(ctx context.Context, terms [][]byte) (map[string][]int, error) {
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

func (i Index) WipeFile(file string) error {
	err := i.WipeSegments(file)
	if err != nil {
		return fmt.Errorf("db wipe segments: %w", err)
	}
	return i.DuckDB.WipeFile(file)
}

func (i Index) WipeSegments(file string) error {
	ids, err := i.DuckDB.WipeSegments(file)
	if err != nil {
		return fmt.Errorf("db wipe segments: %w", err)
	}
	idsUint32 := make([]uint32, 0, len(ids))
	for _, id := range ids {
		idsUint32 = append(idsUint32, uint32(id))
	}
	return i.ii.PutRemoved(idsUint32)
}

func (i Index) WipeSegment(file string, segment common.Location) error {
	id, err := i.DuckDB.WipeSegment(file, segment)
	if err != nil {
		return fmt.Errorf("db wipe segment: %w", err)
	}

	err = i.ii.PutRemoved([]uint32{uint32(id)})
	if err != nil {
		return fmt.Errorf("inverted index delete: %w", err)
	}

	return nil
}

func (i Index) PutSegment(file string, terms [][]byte, messages []common.Message) (int, error) {
	segmentId, err := i.DuckDB.PutSegment(file, messages)
	if err != nil {
		return segmentId, fmt.Errorf("db put segment: %w", err)
	}

	err = i.ii.Put(terms, uint32(segmentId))
	if err != nil {
		_, _ = i.DuckDB.WipeSegment(file, messages[0].Loc)
		return segmentId, fmt.Errorf("inverted index put terms: %w", err)
	}

	return segmentId, nil
}
