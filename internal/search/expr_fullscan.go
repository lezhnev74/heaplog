package search

import (
	"heaplog_2024/internal/search/query_language"
)

// ShouldFullScan given user expression, tests it if can use the inverted index for performing the search.
// Expression's leaves are terms that we can exchange for indexed segments.
// But, sometimes the index can not be used. In which case a full-scan is performed.
// The index won't help for regular expressions, so we perform expression analysis to see if
// full-scan is unavoidable. Also short terms (below indexable length) lead to full-scan.
func ShouldFullScan(expr *query_language.Expression, tokenize func([]byte) [][]byte) bool {
	var m func(e *query_language.Expression) bool
	m = func(e *query_language.Expression) (isFullScan bool) {
		if e.Operator == query_language.NOT {
			return true
		}

		collapseFn := func(prev, cur bool) bool { return prev || cur }
		if e.Operator == query_language.AND {
			collapseFn = func(prev, cur bool) bool { return prev && cur }
		}
		var opValue bool
		for i, operand := range e.Operands {
			switch o := operand.(type) {
			case *query_language.Expression:
				opValue = m(o)
			case string:
				opValue = len(tokenize([]byte(o))) == 0
			case query_language.RegExpLiteral:
				opValue = true
			}

			if i == 0 {
				isFullScan = opValue
			} else {
				isFullScan = collapseFn(isFullScan, opValue)
			}
		}

		return
	}
	return m(expr)
}
