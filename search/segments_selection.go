package search

import (
	"fmt"
	"golang.org/x/xerrors"
	"heaplog/common"
	"heaplog/storage"
	"math"
	"strconv"
	"strings"
	"time"
)

// SegmentsSelector does segment preselection based on the user query expression
// it uses Inverted Index to reduce the number of segments to scan for the query (if possible).
type SegmentsSelector struct {
	// invertedIndexesRootDir is where all indexes are stored, each data source has its own subdirectory
	storage *storage.Storage
	// unboundedTokenizer is used to find similar terms, so it should not have a lower limit on literal size
	// example: "er" is not a term but if found in the query should match "error" term in the storage.
	unboundedTokenizer func(input string) []string
	tokenizer          func(input string) []string
}

func NewSegmentSelector(
	storage *storage.Storage,
	unboundedTokenizer func(input string) []string,
	tokenizer func(input string) []string,
) *SegmentsSelector {
	return &SegmentsSelector{
		storage:            storage,
		unboundedTokenizer: unboundedTokenizer,
		tokenizer:          tokenizer,
	}
}

// SelectSegments returns indexed segments that are relevant to the given expression and date bounds.
func (s *SegmentsSelector) SelectSegments(expr *Expression, minDate, maxDate *time.Time) (
	// selection tree returns oldest segments first
	segmentIds []int,
	err error,
) {
	// Prepare the query for similarity search
	// normal tokenizer is used for confident usage of II (shorter literals means full-scan and no II can be used)
	similarExpr := expr.clone()
	similarExpr.mapKeyword(AND, s.tokenizer)
	segmentsMap, err := s.selectTermSegments(similarExpr.findKeywords())
	if err != nil {
		return
	}

	segmentIds, err = s.evalExprViaCTE(expr, segmentsMap, minDate, maxDate)
	return
}

// evalExprViaCTE it filters given termSegments by evaluating the expr.
// it evaluates the expr via duckdb CTE
func (s *SegmentsSelector) evalExprViaCTE(
	// expr must contain tokenized literals (so any user "x1.me.12" should be tokenized to {"x1","me"}
	expr *Expression,
	// for each literal in the expr it contains segments id
	// (used to identify segments)
	termSegments map[string][]uint64,
	minDate, maxDate *time.Time,
) (
	segmentIds []int,
	err error,
) {

	whereSQL := make([]string, 0, 3)
	whereSQL = append(whereSQL, "1=1")
	if minDate != nil {
		whereSQL = append(whereSQL, fmt.Sprintf(`dateMax>=%d`, minDate.UnixMicro()))
	}
	if maxDate != nil {
		whereSQL = append(whereSQL, fmt.Sprintf(`dateMin<=%d`, maxDate.UnixMicro()))
	}

	lastExprIndex := 0
	cetSql, _, err := s.exprToCte(&lastExprIndex, expr, termSegments, strings.Join(whereSQL, " AND "))
	if err != nil {
		return nil, xerrors.Errorf("finalize segments failed: %w", err)
	}

	finalSql := fmt.Sprintf(`select id from file_segments where %s`, strings.Join(whereSQL, " AND "))
	if lastExprIndex != 0 {
		finalSql = fmt.Sprintf(`
-- CTE goes below --------------------
WITH
%s 
-- CTE goes above --------------------
-- expr%d - expr root node

%s AND id IN (select * from expr%[2]d) ORDER BY dateMin ASC
`,
			strings.TrimRight(cetSql, ","),
			lastExprIndex,
			finalSql,
		)
	}

	// log.Printf(finalSql)

	r, err := s.storage.GetDb().Query(finalSql)
	if err != nil {
		return nil, err
	}

	var id int
	for r.Next() {
		err = r.Scan(&id)
		if err != nil {
			err = xerrors.Errorf("unable to read existing file segments: %w", err)
			return
		}
		segmentIds = append(segmentIds, id)
	}

	return segmentIds, err
}

// exprToCte is called recursively on expr tree to make a single string
// containing CTEs
func (s *SegmentsSelector) exprToCte(
	lastCteIndex *int,
	exprNode any,
	termSegments map[string][]uint64,
	whereSQL string,
) (
	cte string,
	isFullScan bool, // used in downstream optimizations (like "NOT fullScan")
	err error,
) {

	// If the node is a regular expression -> no pre-selection is possible: full-scan
	if _, ok := exprNode.(regExpLiteral); ok {
		*lastCteIndex++
		return fmt.Sprintf("expr%d AS (SELECT id FROM file_segments WHERE %s),", *lastCteIndex, whereSQL), true, nil
	}

	// evaluate leaves of the expr tree (literals)
	// literals are not escaped or tokenized, could be like "a&b@"
	if term, ok := exprNode.(string); ok {
		tokens := s.tokenizer(term)

		// a short literal that cant be tokenizer means we have to use full-scan:
		// "er" will match tokens like "error" in II, but there could be messages like "me me me".
		// such messages are invisible for II and must be full-scanned.
		if len(tokens) == 0 {
			*lastCteIndex++
			return fmt.Sprintf("expr%d AS (SELECT id FROM file_segments WHERE %s),", *lastCteIndex, whereSQL), true, nil
		}

		// At this point we know that literals are long enough to be visible for II
		// so, we apply similar terms or if nothing found we say that the literals matches no segments.

		if len(tokens) > 1 {
			// make a new sub-expression like "a AND b" and evaluate here
			subExpr := &Expression{
				operator: AND,
				operands: common.SliceToAny(tokens),
			}
			return s.exprToCte(lastCteIndex, subExpr, termSegments, whereSQL)
		}

		// if tokenization returns just a single token then proceed with no further recursion.
		*lastCteIndex++
		term = tokens[0]

		segmentPositions, ok := termSegments[term]
		if !ok {
			return fmt.Sprintf("expr%d AS (select null where 0),", *lastCteIndex), false, nil
		}

		posWhereSql := make([]string, 0, len(segmentPositions))
		for _, pos := range segmentPositions {
			posWhereSql = append(posWhereSql, strconv.Itoa(int(pos)))
		}

		// todo: note that here we can do constants like: "VALUES (pos1),(pos2),..." if time constraints are applied in pre-selection
		// ex: "expr%d AS (VALUES %s),",
		return fmt.Sprintf(
			"expr%d AS (SELECT id FROM file_segments WHERE id IN (%s) AND %s),",
			*lastCteIndex,
			strings.Join(posWhereSql, ","),
			whereSQL,
		), false, nil
	}

	expr, ok := exprNode.(*Expression)
	if !ok {
		return "", false, xerrors.Errorf("query expr tree node has unexpected type %T", expr)
	}

	sets := make([]string, 0)
	isFullScan = true // default to true, then inherit what operands return
	for _, operand := range expr.operands {
		operandCte, opIsFullscan, err := s.exprToCte(lastCteIndex, operand, termSegments, whereSQL)
		if err != nil {
			return "", false, err
		}
		isFullScan = isFullScan && opIsFullscan
		cte += operandCte + "\n"
		sets = append(sets, fmt.Sprintf(`SELECT * FROM "expr%d"`, *lastCteIndex))
	}

	curCte := ""
	switch expr.operator {
	case AND:
		curCte = strings.Join(sets, "\nINTERSECT\n")
	case OR:
		curCte = strings.Join(sets, "\nUNION\n")
	case NOT:
		if len(expr.operands) > 1 {
			return "", false, xerrors.Errorf("operand NOT is unary but found %d operands", len(expr.operands))
		}

		// negation makes no sense as we work on segment basis.
		// each segment contains individual messages each must be tested.
		// so only direct selection is possible.

		curCte = fmt.Sprintf("select id FROM file_segments WHERE %s", whereSQL)
	}

	if curCte == "" {
		return "", false, nil
	}

	*lastCteIndex++
	cte += fmt.Sprintf("expr%d AS (\n%s\n),", *lastCteIndex, curCte)

	return
}

// selectTermSegments finds relevant segments for each literal by expanding to similar terms and querying the II.
// later this is used in expr evaluation, each operand is mapped to relevant segments:
// ex: "A AND B" -> (segment1,segment2) AND (segment2,segment9) = (segment2)
func (s *SegmentsSelector) selectTermSegments(
	// expr must contain tokenized literals (so any user "x1.me.12" should be tokenized to {"x1","me","12"}
	literals []string,
) (
	// the map contains segments per literal
	relevantSegments map[string][]uint64,
	err error,
) {
	relevantSegments = make(map[string][]uint64)

	if len(literals) == 0 {
		// unable to use index as no terms inferred from the query expr
		return relevantSegments, nil
	}

	// the rest of the algorithm is done concurrently per-file
	var similarTerms map[string][]uint64
	similarTerms, err = s.storage.ReadTermsLike(literals)
	if err != nil {
		return nil, xerrors.Errorf("select term segments: %w", err)
	}

	// todo: apply min/max segment here based on user settings
	// map min/max Date to min/max pos and read from InvIndex

	for literal, similars := range similarTerms {
		segmentIds, err := s.storage.ReadSegmentIdsFromTerms(similars, 0, math.MaxUint64)
		if err != nil {
			return nil, xerrors.Errorf("select term segments: %w", err)
		}
		if len(segmentIds) == 0 {
			continue
		}
		relevantSegments[literal] = segmentIds
	}

	return
}
