package terms

import (
	"bytes"
	"fmt"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"math/rand"
	"slices"
	"testing"
)

func TestRebuild(t *testing.T) {
	terms1 := make([]string, 0, 500)
	for i := 0; i < cap(terms1); i++ {
		terms1 = append(terms1, fmt.Sprintf("t%d", rand.Int()))
	}

	terms2 := make([]string, 0, 500)
	for i := 0; i < cap(terms2); i++ {
		terms2 = append(terms2, fmt.Sprintf("t%d", rand.Int()))
	}

	tt := NewTerms()
	_, err := tt.Put(terms1)
	require.NoError(t, err)
	_, err = tt.Put(terms2)
	require.NoError(t, err)
}

func TestTermIdsUnique(t *testing.T) {
	// In newTerms
	terms := NewTerms()

	_, err := terms.Put([]string{"message", "first"})
	require.NoError(t, err)

	_, err = terms.Put([]string{"last"})
	require.NoError(t, err)

	_, err = terms.Put([]string{"multiline"})
	require.NoError(t, err)

	allTerms := go_iterators.ToSlice(terms.All())

	allIds := make([]uint64, 0)
	for _, t := range allTerms {
		allIds = append(allIds, t.id)
	}
	slices.Sort(allIds)
	allIds = slices.Compact(allIds)

	require.Equal(t, len(allTerms), len(allIds))

}

func TestTermValuesRemain(t *testing.T) {
	// In newTerms
	terms := NewTerms()
	ids, err := terms.Put([]string{"term1", "term2"})
	require.NoError(t, err)

	_id1, ok := terms.Get("term1")
	require.True(t, ok)
	require.Equal(t, _id1, ids[0])

	_id2, ok := terms.Get("term2")
	require.True(t, ok)
	require.Equal(t, _id2, ids[1])

	actualTerms := go_iterators.ToSlice(terms.All())
	expectedTerms := []termId{
		{"term1", 1},
		{"term2", 2},
	}
	require.Equal(t, expectedTerms, actualTerms)

	// Restore
	terms = restoreTerms(t, terms)

	_id1, ok = terms.Get("term1")
	require.True(t, ok)
	require.Equal(t, _id1, ids[0])

	_id2, ok = terms.Get("term2")
	require.True(t, ok)
	require.Equal(t, _id2, ids[1])

	// Put again
	ids3, err := terms.Put([]string{"term3"})
	require.NoError(t, err)
	terms = restoreTerms(t, terms)

	_id3, ok := terms.Get("term3")
	require.True(t, ok)
	require.Equal(t, _id3, ids3[0])

	// expect ids go from 1
	require.Equal(t, _id1, ids[0])
	require.Equal(t, uint64(1), ids[0])

	require.Equal(t, _id2, ids[1])
	require.Equal(t, uint64(2), ids[1])

	require.Equal(t, _id3, ids3[0])
	require.Equal(t, uint64(3), ids3[0])
}

func TestRestoredTerms(t *testing.T) {
	terms := prepareNewTerms([]string{"term1", "term2"})
	terms = restoreTerms(t, terms)

	// Put
	_, err := terms.Put([]string{"term2", "term3", "term1", "term0"})
	require.NoError(t, err)

	// Assert
	actualTerms := go_iterators.ToSlice(terms.All())
	expectedTerms := []termId{
		{"term0", 3},
		{"term1", 1},
		{"term2", 2},
		{"term3", 4},
	}
	require.Equal(t, expectedTerms, actualTerms)
}

func restoreTerms(t *testing.T, terms *Terms) *Terms {
	buf := bytes.NewBuffer(nil)
	_, err := terms.WriteTo(buf)
	require.NoError(t, err)

	terms, err = NewTermsFrom(buf)
	require.NoError(t, err)

	return terms
}

func prepareNewTerms(putTerms []string) *Terms {
	terms := NewTerms()
	terms.Put(putTerms)
	return terms
}
