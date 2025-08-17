package query_language

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"heaplog_2024/internal/search/query_language/query_antlr"
)

var errorUserQueryInvalidSyntax = fmt.Errorf("invalid query_language syntax")

// RegExpLiteral is a string that contains regular expression as given from the user
type RegExpLiteral string

type AntlrListener struct {
	query_antlr.BaseQueryLanguageListener
	qe *Expression
}

func (s *AntlrListener) handle(node any) (ret any) {

	mapExprs := func(exprs []query_antlr.IExprContext) (qes []any) {
		qes = make([]any, 0)
		for _, expr := range exprs {
			qes = append(qes, s.handle(expr))
		}
		return
	}

	switch c := node.(type) {
	case *query_antlr.ExprRELiteralContext:
		literal := c.GetText()                     // final RE LITERAL
		literal = strings.TrimPrefix(literal, "~") // remove the Operator "~"
		if len(literal) > 1 && literal[0] == literal[len(literal)-1] && strings.ContainsAny(literal[0:1], `"'`) {
			literal = strings.Trim(literal, string(literal[0])) // remove quotes if any
		}
		ret = RegExpLiteral(literal)
	case *query_antlr.ExprLiteralContext:
		literal := c.GetText() // final LITERAL
		if len(literal) > 1 && literal[0] == literal[len(literal)-1] && strings.ContainsAny(literal[0:1], `"'`) {
			literal = strings.Trim(literal, string(literal[0])) // remove quotes if any
		}
		ret = literal
	case *query_antlr.ExprAndContext:
		ret = &Expression{
			Operator: AND,
			Operands: mapExprs(c.AllExpr()),
		}
	case *query_antlr.ExprOrContext:
		ret = &Expression{
			Operator: OR,
			Operands: mapExprs(c.AllExpr()),
		}
	case *query_antlr.ExprNotContext:
		ret = &Expression{
			Operator: NOT,
			Operands: mapExprs([]query_antlr.IExprContext{c.Expr()}),
		}
	case *query_antlr.ExprGroupContext:
		ret = s.handle(c.Expr())
	default:
		ret = nil
	}

	return
}

func (s *AntlrListener) EnterQuery(ctx *query_antlr.QueryContext) {
	// Start with AND Operator
	operands := []any{}
	for _, node := range ctx.GetChildren() {
		operand := s.handle(node)
		if operand == nil {
			continue // not the node that can be evaluated
		}
		operands = append(operands, operand)
	}

	if len(operands) == 0 {
		return //
	}

	qe := &Expression{
		Operator: AND,
		Operands: operands,
	}
	s.qe = qe.optimize() // final step, optimize the final QE tree
}

type AntlrErrorListener struct {
	*antlr.DefaultErrorListener
	syntaxError error
}

func (el *AntlrErrorListener) SyntaxError(
	recognizer antlr.Recognizer,
	offendingSymbol interface{},
	line, column int,
	msg string,
	e antlr.RecognitionException,
) {
	el.syntaxError = fmt.Errorf("%s", "line "+strconv.Itoa(line)+":"+strconv.Itoa(column)+" "+msg)
}

func ParseUserQuery(query string) (*Expression, error) {

	// edge-case: an empty query_language
	query = strings.Trim(query, "\t\n\r")
	if query == "" {
		return &Expression{AND, []any{}}, nil
	}

	listener := &AntlrListener{}

	input := antlr.NewInputStream(query)
	lexer := query_antlr.NewQueryLanguageLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)

	errorListener := new(AntlrErrorListener)
	p := query_antlr.NewQueryLanguageParser(stream)
	p.RemoveErrorListeners()
	p.AddErrorListener(errorListener)
	p.BuildParseTrees = true
	tree := p.Query()

	antlr.ParseTreeWalkerDefault.Walk(listener, tree)

	if errorListener.syntaxError != nil {
		return nil, fmt.Errorf("%s: %w", errorUserQueryInvalidSyntax, errorListener.syntaxError)
	}

	return listener.qe, nil
}
