package search

import (
	"slices"

	"heaplog_2024/internal/search/query_language"
)

var allSegmentsMarker = -1
var allSegmentsSuperset = []int{allSegmentsMarker}

// exprEval evaluates the given user expression to determine relevant segments for search.
// Each term in the expression is mapped to a set of segments, and evaluation is performed
// through set operations (AND/OR) on these segment sets. The expression should be
// pre-normalized and have its literals mapped to segment sets. Special handling is
// implemented for the allSegmentsSuperset case, which indicates a full scan is needed.
// The NOT operator always returns allSegmentsSuperset since negation alone cannot
// determine relevant segments.
func exprEval(expr *query_language.Expression) (segments []int) {

	var m func(e *query_language.Expression) []int
	m = func(e *query_language.Expression) []int {

		operands := make([][]int, 0, len(e.Operands))
		for _, operand := range e.Operands {
			switch o := operand.(type) {
			case *query_language.Expression:
				operands = append(operands, m(o))
			case []int:
				operands = append(operands, o)
			default:
				panic("expr nodes are not sets")
			}
		}

		switch e.Operator {
		case query_language.OR:
			return setOr(operands)
		case query_language.AND:
			return setAnd(operands)
		case query_language.NOT:
			// inversion does not say anything about relevant segments
			return allSegmentsSuperset
		}

		panic("unexpected operator in expr eval")
	}

	return m(expr)
}

func setOr(sets [][]int) (r []int) {

	for i := 0; i < len(sets); i++ {
		if slices.Equal(sets[i], allSegmentsSuperset) {
			return allSegmentsSuperset
		}
	}

	l := 0
	for i := 0; i < len(sets); i++ {
		l += len(sets[i])
	}

	r = make([]int, 0, l)
	for i := 0; i < len(sets); i++ {
		r = append(r, sets[i]...)
	}

	slices.Sort(r)
	r = slices.Compact(r)

	return
}

func setAnd(sets [][]int) (r []int) {

	if len(sets) == 0 {
		return
	} else if len(sets) == 1 {
		return sets[0]
	}

	r1 := sets[0]
	r2 := setAnd(sets[1:])

	if slices.Equal(r1, allSegmentsSuperset) {
		return r2
	}
	if slices.Equal(r2, allSegmentsSuperset) {
		return r1
	}

	slices.Sort(r1)
	slices.Sort(r2)

	if len(r1) == 0 {
		return r2
	}

	for _, v := range r1 {
		_, ok := slices.BinarySearch(r2, v)
		if ok {
			r = append(r, v)
		}
	}

	r = slices.Compact(r)

	return
}

// exprMapLiteralsToSets transforms string literals and regular expressions in the query expression
// into segment sets that can be used for evaluation. For string literals, it tokenizes the input
// and maps each token to its corresponding segment set from the inverted index (termValues),
// combining them with AND operations. If a string literal produces no tokens, it maps to
// allSegmentsSuperset indicating a full scan is required. Regular expressions are also mapped
// to allSegmentsSuperset as they require full scanning of segments.
func exprMapLiteralsToSets(
	expr *query_language.Expression,
	tokenize func([]byte) [][]byte,
	termValues map[string][]int,
) (exprClone *query_language.Expression) {
	exprClone = expr.Clone()
	exprClone.Visit(
		func(expr *query_language.Expression) {
			for i, operand := range expr.Operands {
				switch tl := operand.(type) {
				case string:
					// normal term, if short -> Full-Scan
					terms := tokenize([]byte(tl))
					if len(terms) == 0 {
						expr.Operands[i] = allSegmentsSuperset // no long prefix-terms => Full-Scan
						continue
					}
					// otherwise, AND-combine results from II
					sets := make([]any, 0, len(terms))
					for _, term := range terms {
						termSet, ok := termValues[string(term)]
						if !ok {
							continue
						}
						sets = append(sets, termSet)
					}
					expr.Operands[i] = &query_language.Expression{Operator: query_language.AND, Operands: sets}
				case query_language.RegExpLiteral:
					expr.Operands[i] = allSegmentsSuperset // Full-Scan
				}
			}
		},
	)
	return
}
