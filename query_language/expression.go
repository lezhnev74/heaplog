package query_language

import (
	"fmt"
	"heaplog_2024/common"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	AND = iota + 1
	OR
	NOT
)

type MatchFunc func(string) bool

type operator int8

// Expression is just a tree that represents user's QuerySearch query_language
// Later we apply different transformations on top of it for QuerySearch purposes
type Expression struct {
	Operator operator
	Operands []any
}

// optimize applies some simplification algorithms To the original tree
func (qe *Expression) optimize() (optimized *Expression) {
	if qe == nil {
		return qe
	}

	optimized = qe
	for keep := true; keep; {
		keep = optimized.mergeSimilarParent()
	}

	// RemoveQuery AND with a single argument (for the top-most rule may give this)
	for keep := true; keep; {
		keep = optimized.removeUnaryAND()
	}

	return optimized
}

func (qe *Expression) convertUnaryAndToOr() (optimized bool) {
	// Unary OR is much simpler in processing (matching) rather than unary AND
	// as unary OR is substituted with (<empty> OR Some) rather than (ALL AND Some)
	for _, operand := range qe.Operands {
		if operandQE, ok := operand.(*Expression); ok {
			childOptimized := operandQE.convertUnaryAndToOr()
			if childOptimized {
				optimized = true
			}
		}
	}

	if len(qe.Operands) == 1 && qe.Operator == AND {
		*qe, optimized = Expression{OR, qe.Operands}, true
	}

	return
}

func (qe *Expression) removeUnaryAND() (optimized bool) {
	// find AND with a single expression child
	// check if this node is optimizable
	if len(qe.Operands) == 1 && qe.Operator == AND {
		if operandQE, ok := qe.Operands[0].(*Expression); ok {
			*qe, optimized = *operandQE, true
		}
	}

	// otherwise apply To QE children
	for _, operand := range qe.Operands {
		if operandQE, ok := operand.(*Expression); ok {
			optimized = operandQE.removeUnaryAND()
		}
	}

	return
}

// mergeSimilarParent merges similar child-parent nodes: AND(1,AND(2,3)) => AND(1,2,3)
func (qe *Expression) mergeSimilarParent() (optimized bool) {
	// recursive To the children
	for _, operand := range qe.Operands {
		operandQE, ok := operand.(*Expression)
		if !ok {
			continue
		}
		childOptimized := operandQE.mergeSimilarParent()
		optimized = optimized || childOptimized
	}

	// If all children are the same, then we can merge
	accumulatedOperands := make([]any, 0)
	nonOptimizable := true
	for _, operand := range qe.Operands {

		operandQE, ok := operand.(*Expression)
		if !ok {
			accumulatedOperands = append(accumulatedOperands, operand)
			continue
		}

		// special case for (And and Not)
		if qe.Operator == AND && operandQE.Operator == NOT {
			accumulatedOperands = append(accumulatedOperands, operandQE)
			continue
		}

		// otherwise
		if operandQE.Operator != qe.Operator {
			return // stop
		}
		accumulatedOperands = append(accumulatedOperands, operandQE.Operands...)
		nonOptimizable = false
	}

	if nonOptimizable {
		return
	}

	qe.Operands = accumulatedOperands
	return true
}

func (qe *Expression) Visit(visit func(*Expression)) {
	for _, operand := range qe.Operands {
		switch opExpr := operand.(type) {
		case *Expression:
			opExpr.Visit(visit)
		}
	}
	visit(qe)
}

// FindKeywords returns all leaf strings (= literals), except RE
func (qe *Expression) FindKeywords() []string {
	ret := make([]string, 0)
	qe.Visit(func(expr *Expression) {
		for _, operand := range expr.Operands {
			if literal, ok := operand.(string); ok {
				ret = append(ret, literal)
			}
		}
	})
	return ret
}

// MapLiterals maps all literal-leaves
func (qe *Expression) MapLiterals(mapFunc func(literal any) any) {
	qe.Visit(func(expr *Expression) {
		for i, operand := range expr.Operands {
			if expr, ok := operand.(*Expression); ok {
				expr.MapLiterals(mapFunc)
				continue
			}
			expr.Operands[i] = mapFunc(operand)
		}
	})
	qe.optimize()
}

func (qe *Expression) Hash() string {
	clone := qe.Clone()
	clone.sort()
	qeString := ""

	clone.Visit(func(qe *Expression) {
		qeString += strconv.Itoa(int(qe.Operator))
		for _, operand := range qe.Operands {
			if _, ok := operand.(*Expression); ok {
				return
			} else if _, ok := operand.(RegExpLiteral); ok {
				qeString += "~" // regexp literal must not be equal to a normal literal
			}
			qeString += fmt.Sprintf("%v", operand) // assume all literals are strings
		}
	})

	return common.HashString(qeString)
}

// Clone creates a copy of the original expression
func (qe *Expression) Clone() *Expression {
	operands := make([]any, len(qe.Operands))
	for i, operand := range qe.Operands {
		operands[i] = operand // anything is copied by value
		if qeOperand, ok := operand.(*Expression); ok {
			operands[i] = qeOperand.Clone() // except *Expression, which is cloned
		}
	}
	return &Expression{qe.Operator, operands}
}

// sort changes Operands order in place
func (qe *Expression) sort() {
	qe.Visit(func(expr *Expression) {
		sort.SliceStable(expr.Operands, func(i, j int) bool {
			if _, ok := expr.Operands[j].(*Expression); ok {
				return true // expression is always bigger than anything else
			}
			if _, ok := expr.Operands[i].(*Expression); ok {
				return false
			}
			// both are strings
			leftLiteral := fmt.Sprintf("%v", expr.Operands[i])
			rightLiteral := fmt.Sprintf("%v", expr.Operands[j])
			return leftLiteral < rightLiteral
		})
	})
}

// GetMatcher returns a function to match the expression against a string.
// that is used in a 2-phase query_language to make the final matching of messages (strings)
func (qe *Expression) GetMatcher() MatchFunc {

	var expr2match func(qe *Expression) MatchFunc
	expr2match = func(qe *Expression) MatchFunc {

		operandFuncs := make([]func(string) bool, 0, len(qe.Operands))

		for _, operand := range qe.Operands {
			var operandFunc func(string) bool

			switch o := operand.(type) {
			case string:
				o = strings.ToLower(o)
				operandFunc = func(s string) bool {
					s = strings.ToLower(s)
					return strings.Contains(s, o) // case-sensitive matching
				}
			case RegExpLiteral:
				p := regexp.MustCompile(string(o)) // RE match
				operandFunc = p.MatchString
			case *Expression:
				operandFunc = expr2match(o)
			}

			operandFuncs = append(operandFuncs, operandFunc)
		}

		return func(message string) bool {
			switch qe.Operator {
			case AND:
				for _, opFunc := range operandFuncs {
					if opFunc(message) == false {
						return false
					}
				}
				return true
			case OR:
				for _, opFunc := range operandFuncs {
					if opFunc(message) == true {
						return true
					}
				}
				return false
			case NOT:
				for _, opFunc := range operandFuncs {
					if opFunc(message) == true {
						return false
					}
				}
				return true
			default:
				panic(fmt.Sprintf("unsupported Operator %d in a GetMatcher function", qe.Operator))
			}
		}
	}

	return expr2match(qe)
}

func (qe *Expression) String() string {
	s := ""

	if qe == nil {
		return s
	}

	sOps := make([]string, 0, len(qe.Operands))
	for _, operand := range qe.Operands {
		switch op := operand.(type) {
		case string:
			sOps = append(sOps, op)
		case RegExpLiteral:
			sOps = append(sOps, fmt.Sprintf("~%s", op))
		case *Expression:
			sOps = append(sOps, op.String())
		}
	}

	switch qe.Operator {
	case AND:
		s += fmt.Sprintf("AND(%s)", strings.Join(sOps, ","))
	case OR:
		s += fmt.Sprintf("OR(%s)", strings.Join(sOps, ","))
	case NOT:
		s += fmt.Sprintf("NOT(%s)", strings.Join(sOps, ","))
	}

	return s
}
