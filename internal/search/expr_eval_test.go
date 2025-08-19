package search

import (
	"fmt"
	"log"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

func TestSetOps(t *testing.T) {

	// AND
	r := setAnd(
		[][]int{
			{1, 2, 3},
			{2, 3, 4},
		},
	)
	require.Equal(t, []int{2, 3}, r)

	// OR
	r = setOr(
		[][]int{
			{1, 2, 3},
			{2, 3, 4},
		},
	)
	require.Equal(t, []int{1, 2, 3, 4}, r)

}

func TestExprEval(t *testing.T) {
	type test struct {
		query            string
		allSegments      []int
		termSegments     map[string][]int
		expectedSegments []int
	}

	tokenize := func(literal []byte) [][]byte {
		return common.Tokenize(literal, 4, 8)
	}

	tests := []test{
		{ // Test single term query matching multiple segments
			query: "error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{1, 2, 3},
		},
		{ // Test negation of single term requires full scan
			query: "!error",
			termSegments: map[string][]int{
				"error": {1},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test regex pattern requires full scan of segments
			query: "~error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test regex pattern requires full scan of segments
			query: "@error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test implicit AND of same term preserves segments
			query: "error error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{1, 2, 3},
		},
		{ // Test explicit OR of same term preserves segments
			query: "error OR error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{1, 2, 3},
		},
		{ // Test implicit AND between different terms returns intersection
			query: "error failure",
			termSegments: map[string][]int{
				"error":   {1, 2, 3},
				"failure": {2, 6},
			},
			expectedSegments: []int{2},
		},
		{ // Test OR between different terms returns union
			query: "error OR failure",
			termSegments: map[string][]int{
				"error":   {1, 2, 3},
				"failure": {2, 6},
			},
			expectedSegments: []int{1, 2, 3, 6},
		},
		{ // Test term with negated term requires post-filtering
			// one segment can contain messages with "failure" as well without,
			// we can't discard segments just based on the II.
			query: "error !failure",
			termSegments: map[string][]int{
				"error":   {1, 2},
				"failure": {2, 3},
			},
			expectedSegments: []int{1, 2},
		},
		{ // Test OR between regex and term requires full scan
			query: "~error OR error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test AND between regex and term uses term segments
			query: "~error error",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{1, 2, 3},
		},
		{ // Test negated regex requires full scan validation
			query: "!(~error)",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test negation of regex AND term requires full scan
			query: "!(~error error)",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test negation of regex OR term requires full scan
			query: "!(~error OR error)",
			termSegments: map[string][]int{
				"error": {1, 2, 3},
			},
			expectedSegments: []int{allSegmentsMarker},
		},
		{ // Test complex expression with negation, regex, OR and AND
			query: "!(~error OR error) AND failure",
			termSegments: map[string][]int{
				"error":   {1, 2, 3},
				"failure": {1, 5},
			},
			expectedSegments: []int{1, 5},
		},
	}

	for i, tt := range tests {
		t.Run(
			fmt.Sprintf("Test %d", i), func(t *testing.T) {
				expr, err := query_language.ParseUserQuery(tt.query)
				require.NoError(t, err)
				mappedExpr := exprMapLiteralsToSets(expr, tokenize, tt.termSegments)
				log.Printf("%s", mappedExpr.String())
				segments := exprEval(mappedExpr)
				require.Equal(t, tt.expectedSegments, segments)

				if slices.Equal(segments, allSegmentsSuperset) {
					require.True(t, shouldFullScan(expr, tokenize))
				}
			},
		)
	}
}
