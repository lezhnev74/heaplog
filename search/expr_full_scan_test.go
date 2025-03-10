package search

import (
	"testing"

	"github.com/stretchr/testify/require"

	"heaplog_2024/query_language"
	"heaplog_2024/tokenizer"
)

func TestFullScanDetection(t *testing.T) {

	tokenize := func(s []byte) [][]byte {
		return tokenizer.Tokenize(s, 4, 8)
	}

	type test struct {
		query      string
		isFullScan bool
	}

	tests := []test{
		// FULL-SCAN
		{"err", true},            // too short
		{"абв", true},            // too short unicode
		{"~err", true},           // regular expression
		{"error OR ~err", true},  // OR-union with a Full-FilterMessagesStream
		{"!error OR ~err", true}, // OR-union with a Full-FilterMessagesStream
		{"(error and failure) OR ((message AND long) OR ~err) ", true}, // regular expression in a complex tree
		// INVERTED INDEX:
		{"error", false},                   // valid term
		{"!error", false},                  // NOT-operator
		{"error AND ~err", false},          // AND-union with a valid term
		{"error OR failure", false},        // AND-union with a valid term
		{"left AND (~re OR right)", false}, // AND-union with a valid term in a complex tree
		{"(error and failure) AND ((message AND long) OR ~err) ", false}, // AND-union with a valid term in a complex tree
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := query_language.ParseUserQuery(tt.query)
			require.NoError(t, err)

			isFullScan := ShouldFullScan(expr, tokenize)
			require.Equal(t, tt.isFullScan, isFullScan)
		})
	}
}
