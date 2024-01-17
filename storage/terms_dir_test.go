package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestMultiplePut(t *testing.T) {
	// Plan:
	// 1. Put terms twice: make 2 terms files
	// 2. Read all terms: combine reading from 2 files
	// 3. Assert all terms are returned, make sure ids are consistent

	// Exec:
	// 1. Put terms twice: make 2 terms files
	terms1 := []string{"term1", "term2"}
	terms2 := []string{"term0", "term2", "term3"}

	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	termsDir, err := NewTermsDir(dir)
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
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	termsDir, err := NewTermsDir(dir)
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
	// 1. Put terms twice: make 2 terms files
	terms1 := []string{"term1", "term2"}
	terms2 := []string{"term0", "term2", "term3"}

	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	termsDir, err := NewTermsDir(dir)
	require.NoError(t, err)

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
