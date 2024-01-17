package search

import (
	"fmt"
	"heaplog/common"
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

type operator int8

// Expression is just a tree that represents user's QuerySearch query
// Later we apply different transformations on top of it for QuerySearch purposes
type Expression struct {
	operator operator
	operands []any
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

	// Remove AND with a single argument (for the top-most rule may give this)
	for keep := true; keep; {
		keep = optimized.removeUnaryAND()
	}

	return optimized
}

func (qe *Expression) convertUnaryAndToOr() (optimized bool) {
	// Unary OR is much simpler in processing (matching) rather than unary AND
	// as unary OR is substituted with (<empty> OR Some) rather than (ALL AND Some)
	for _, operand := range qe.operands {
		if operandQE, ok := operand.(*Expression); ok {
			childOptimized := operandQE.convertUnaryAndToOr()
			if childOptimized {
				optimized = true
			}
		}
	}

	if len(qe.operands) == 1 && qe.operator == AND {
		*qe, optimized = Expression{OR, qe.operands}, true
	}

	return
}

func (qe *Expression) removeUnaryAND() (optimized bool) {
	// find AND with a single expression child
	// check if this node is optimizable
	if len(qe.operands) == 1 && qe.operator == AND {
		if operandQE, ok := qe.operands[0].(*Expression); ok {
			*qe, optimized = *operandQE, true
		}
	}

	// otherwise apply To QE children
	for _, operand := range qe.operands {
		if operandQE, ok := operand.(*Expression); ok {
			optimized = operandQE.removeUnaryAND()
		}
	}

	return
}

// mergeSimilarParent merges similar child-parent nodes: AND(1,AND(2,3)) => AND(1,2,3)
func (qe *Expression) mergeSimilarParent() (optimized bool) {
	// recursive To the children
	for _, operand := range qe.operands {
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
	for _, operand := range qe.operands {

		operandQE, ok := operand.(*Expression)
		if !ok {
			accumulatedOperands = append(accumulatedOperands, operand)
			continue
		}

		// special case for (And and Not)
		if qe.operator == AND && operandQE.operator == NOT {
			accumulatedOperands = append(accumulatedOperands, operandQE)
			continue
		}

		// otherwise
		if operandQE.operator != qe.operator {
			return // stop
		}
		accumulatedOperands = append(accumulatedOperands, operandQE.operands...)
		nonOptimizable = false
	}

	if nonOptimizable {
		return
	}

	qe.operands = accumulatedOperands
	return true
}

func (qe *Expression) visit(visit func(*Expression)) {
	for _, operand := range qe.operands {
		switch opExpr := operand.(type) {
		case *Expression:
			opExpr.visit(visit)
		}
	}
	visit(qe)
}

// findKeywords returns all leaf strings (= literals), except RE
func (qe *Expression) findKeywords() []string {
	ret := make([]string, 0)
	qe.visit(func(expr *Expression) {
		for _, operand := range expr.operands {
			if literal, ok := operand.(string); ok {
				ret = append(ret, literal)
			}
		}
	})
	return ret
}

// mapKeyword maps all string leaves AND removes those that are mapped To nil
// when one literal is mapped To multiple we need To provide the operator To use (OR or AND)
func (qe *Expression) mapKeyword(op operator, mapFunc func(string) []string) {
	qe.visit(func(expr *Expression) {
		mappedOperands := make([]any, 0)

		for _, operand := range expr.operands {
			literal, ok := operand.(string)
			if !ok {
				mappedOperands = append(mappedOperands, operand)
				continue
			}

			mappedLiterals := mapFunc(literal)

			switch len(mappedLiterals) {
			case 0: // nothing, remove the operand
			case 1: // replace in place
				mappedOperands = append(mappedOperands, mappedLiterals[0])
			default: // replace with an expression
				newExprOperands := make([]any, len(mappedLiterals))
				for j, mappedLiteral := range mappedLiterals {
					newExprOperands[j] = mappedLiteral
				}
				mappedOperands = append(mappedOperands, &Expression{op, newExprOperands})
			}
		}

		expr.operands = mappedOperands
	})

	qe.optimize()
}

func (qe *Expression) Hash() string {
	clone := qe.clone()
	clone.sort()
	qeString := ""

	clone.visit(func(qe *Expression) {
		qeString += strconv.Itoa(int(qe.operator))
		for _, operand := range qe.operands {
			if _, ok := operand.(*Expression); ok {
				return
			} else if _, ok := operand.(regExpLiteral); ok {
				qeString += "~" // regexp literal must not be equal to a normal literal
			}
			qeString += fmt.Sprintf("%v", operand) // assume all literals are strings
		}
	})

	return common.HashString(qeString)
}

// clone creates a copy of the original expression
func (qe *Expression) clone() *Expression {
	operands := make([]any, len(qe.operands))
	for i, operand := range qe.operands {
		operands[i] = operand // anything is copied by value
		if qeOperand, ok := operand.(*Expression); ok {
			operands[i] = qeOperand.clone() // except *Expression, which is cloned
		}
	}
	return &Expression{qe.operator, operands}
}

// sort changes operands order in place
func (qe *Expression) sort() {
	qe.visit(func(expr *Expression) {
		sort.SliceStable(expr.operands, func(i, j int) bool {
			if _, ok := expr.operands[j].(*Expression); ok {
				return true // expression is always bigger than anything else
			}
			if _, ok := expr.operands[i].(*Expression); ok {
				return false
			}
			// both are strings
			leftLiteral := fmt.Sprintf("%v", expr.operands[i])
			rightLiteral := fmt.Sprintf("%v", expr.operands[j])
			return leftLiteral < rightLiteral
		})
	})
}

// getMatcher returns a function to match the expression against a string.
// that is used in a 2-phase query to make the final matching of messages (strings)
func (qe *Expression) getMatcher() func(string) bool {

	var expr2match func(qe *Expression) func(string) bool
	expr2match = func(qe *Expression) func(string) bool {

		operandFuncs := make([]func(string) bool, 0, len(qe.operands))

		for _, operand := range qe.operands {
			var operandFunc func(string) bool

			switch o := operand.(type) {
			case string:
				// Performance RE over strings is questionable
				// see: https://stackoverflow.com/questions/44595669/does-go-have-a-case-insensitive-string-contains-function
				// RE option:
				// re := regexp.MustCompile(fmt.Sprintf("(?i)%s", regexp.QuoteMeta(o))) // exact match (case-insensitive)
				// operandFunc = re.MatchString

				// String option:
				o = strings.ToLower(o)
				operandFunc = func(s string) bool {
					return strings.Contains(strings.ToLower(s), o)
				}
			case regExpLiteral:
				p := regexp.MustCompile(string(o)) // RE match
				operandFunc = p.MatchString
			case *Expression:
				operandFunc = expr2match(o)
			}

			operandFuncs = append(operandFuncs, operandFunc)
		}

		return func(message string) bool {
			switch qe.operator {
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
				panic(fmt.Sprintf("unsupported operator %d in a getMatcher function", qe.operator))
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

	sOps := make([]string, 0, len(qe.operands))
	for _, operand := range qe.operands {
		switch op := operand.(type) {
		case string:
			sOps = append(sOps, op)
		case regExpLiteral:
			sOps = append(sOps, fmt.Sprintf("~%s", op))
		case *Expression:
			sOps = append(sOps, op.String())
		}
	}

	switch qe.operator {
	case AND:
		s += fmt.Sprintf("AND(%s)", strings.Join(sOps, ","))
	case OR:
		s += fmt.Sprintf("OR(%s)", strings.Join(sOps, ","))
	case NOT:
		s += fmt.Sprintf("NOT(%s)", strings.Join(sOps, ","))
	}

	return s
}
