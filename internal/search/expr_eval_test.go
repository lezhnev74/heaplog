package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

func TestSetOps(t *testing.T) {

	// AND
	r := setAnd(
		[][]uint32{
			{1, 2, 3},
			{2, 3, 4},
		},
	)
	require.Equal(t, []uint32{2, 3}, r)

	// OR
	r = setOr(
		[][]uint32{
			{1, 2, 3},
			{2, 3, 4},
		},
	)
	require.Equal(t, []uint32{1, 2, 3, 4}, r)

	// EXCEPT
	r = setExcept([]uint32{1, 2, 3, 4, 5}, []uint32{3, 9})
	require.Equal(t, []uint32{1, 2, 4, 5}, r)

}

func TestExprEval(t *testing.T) {
	type test struct {
		query            string
		allSegments      []uint32
		termSegments     map[string][]uint32
		expectedSegments []uint32
	}

	tokenize := func(literal []byte) [][]byte {
		return common.Tokenize(literal, 4, 8)
	}

	tests := []test{
		{ // ONE TERM SEGMENTS
			query:       "error",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3},
		},
		{ // TWO TERM SEGMENTS
			query:       "error failure",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error":   {1, 2, 3},
				"failure": {2, 6},
			},
			expectedSegments: []uint32{2},
		},
		{ // TWO TERM SEGMENTS WITH INVERSION
			// one segment can contain messages with "failure" as well without,
			// we can't discard segments just based on the II.
			query:       "error !failure",
			allSegments: []uint32{1, 2, 3},
			termSegments: map[string][]uint32{
				"error":   {1, 2},
				"failure": {2, 3},
			},
			expectedSegments: []uint32{1, 2},
		},
		{ // RE -> FULL-SCAN
			query:       "~error",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3, 4, 5},
		},
		{ // RE OR TERM -> FULL-SCAN
			query:       "~error OR error",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3, 4, 5},
		},
		{ // RE AND TERM -> TERM
			query:       "~error error",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3},
		},
		{ // NOT RE -> Full-FilterMessagesStream (edge case)
			query:       "!(~error)",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3, 4, 5},
		},
		{ // NOT (RE AND TERM) -> NOT (TERM)
			query:       "!(~error error)",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3, 4, 5},
		},
		{
			query:       "!(~error OR error)",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error": {1, 2, 3},
			},
			expectedSegments: []uint32{1, 2, 3, 4, 5},
		},
		{ // COMPLEX
			query:       "!(~error OR error) AND failure",
			allSegments: []uint32{1, 2, 3, 4, 5},
			termSegments: map[string][]uint32{
				"error":   {1, 2, 3},
				"failure": {1, 5},
			},
			expectedSegments: []uint32{1, 5},
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				expr, err := query_language.ParseUserQuery(tt.query)
				require.NoError(t, err)
				ExprMapLiteralsToSets(expr, tokenize, tt.termSegments, tt.allSegments)
				segments := ExprEval(expr, tt.allSegments)
				require.Equal(t, tt.expectedSegments, segments)
			},
		)
	}
}
