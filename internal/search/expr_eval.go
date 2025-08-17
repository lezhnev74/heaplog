package search

import (
	"slices"

	"heaplog_2024/internal/search/query_language"
)

// ExprEval evaluates the given user expression on top of segment sets.
// Every term in the expression is mapped to a set of segments,
// evaluation is done by performing set operations on top of that data.
// expr in the function should already come normalized (tokenizer applied),
// so we need no further expr tweaking for the query to eval.
func ExprEval(
	expr *query_language.Expression,
	allSegments []uint32, // superset for NOT-operations
) (segments []uint32) {

	slices.Sort(allSegments)

	var m func(e *query_language.Expression) []uint32
	m = func(e *query_language.Expression) []uint32 {

		operands := make([][]uint32, 0, len(e.Operands))
		for _, operand := range e.Operands {
			switch o := operand.(type) {
			case *query_language.Expression:
				operands = append(operands, m(o))
			case []uint32:
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
			return allSegments
		}

		panic("unexpected operator in expr eval")
	}

	return m(expr)
}

func setOr(sets [][]uint32) (r []uint32) {

	l := 0
	for i := 0; i < len(sets); i++ {
		l += len(sets[i])
	}

	r = make([]uint32, 0, l)
	for i := 0; i < len(sets); i++ {
		r = append(r, sets[i]...)
	}

	slices.Sort(r)
	r = slices.Compact(r)

	return
}

func setAnd(sets [][]uint32) (r []uint32) {

	if len(sets) == 0 {
		return
	} else if len(sets) == 1 {
		return sets[0]
	}

	r1 := sets[0]
	r2 := setAnd(sets[1:])

	slices.Sort(r1)
	slices.Sort(r2)

	for _, v := range r1 {
		_, ok := slices.BinarySearch(r2, v)
		if ok {
			r = append(r, v)
		}
	}

	r = slices.Compact(r)

	return
}

// superset is supposed to be dedup and sorted.
func setExcept(superSet, set []uint32) (r []uint32) {

	slices.Sort(set)
	r = append([]uint32{}, superSet...)

	for _, v := range set {
		pos, ok := slices.BinarySearch(r, v)
		if ok {
			r = append(r[:pos], r[pos+1:]...)
		}
	}

	return
}

// ExprMapLiteralsToSets transforms string literals and regular expressions in the query expression
// into segment sets that can be used for evaluation. For string literals, it tokenizes the input
// and maps each token to its corresponding segment set from the inverted index (termValues).
// If a string literal produces no tokens or contains a regular expression, it maps to all segments
// indicating a full scan is required.
func ExprMapLiteralsToSets(
	expr *query_language.Expression,
	tokenize func([]byte) [][]byte,
	termValues map[string][]uint32,
	allSegments []uint32,
) {
	expr.Visit(
		func(expr *query_language.Expression) {
			for i, operand := range expr.Operands {
				switch tl := operand.(type) {
				case string:
					// normal term, if short -> Full-Scan
					terms := tokenize([]byte(tl))
					if len(terms) == 0 {
						// no long prefix-terms, so Full-Scan
						expr.Operands[i] = allSegments
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
					expr.Operands[i] = allSegments // Full-Scan
				}
			}
		},
	)
}
