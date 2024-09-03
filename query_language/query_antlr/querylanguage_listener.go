// Code generated from /home/dmitry/Code/go/src/heaplog_2024/query_language/QueryLanguage.g4 by ANTLR 4.13.1. DO NOT EDIT.

package query_antlr // QueryLanguage
import "github.com/antlr4-go/antlr/v4"

// QueryLanguageListener is a complete listener for a parse tree produced by QueryLanguageParser.
type QueryLanguageListener interface {
	antlr.ParseTreeListener

	// EnterQuery is called when entering the query_language production.
	EnterQuery(c *QueryContext)

	// EnterExprAnd is called when entering the ExprAnd production.
	EnterExprAnd(c *ExprAndContext)

	// EnterExprGroup is called when entering the ExprGroup production.
	EnterExprGroup(c *ExprGroupContext)

	// EnterExprOr is called when entering the ExprOr production.
	EnterExprOr(c *ExprOrContext)

	// EnterExprRELiteral is called when entering the ExprRELiteral production.
	EnterExprRELiteral(c *ExprRELiteralContext)

	// EnterExprLiteral is called when entering the ExprLiteral production.
	EnterExprLiteral(c *ExprLiteralContext)

	// EnterExprNot is called when entering the ExprNot production.
	EnterExprNot(c *ExprNotContext)

	// ExitQuery is called when exiting the query_language production.
	ExitQuery(c *QueryContext)

	// ExitExprAnd is called when exiting the ExprAnd production.
	ExitExprAnd(c *ExprAndContext)

	// ExitExprGroup is called when exiting the ExprGroup production.
	ExitExprGroup(c *ExprGroupContext)

	// ExitExprOr is called when exiting the ExprOr production.
	ExitExprOr(c *ExprOrContext)

	// ExitExprRELiteral is called when exiting the ExprRELiteral production.
	ExitExprRELiteral(c *ExprRELiteralContext)

	// ExitExprLiteral is called when exiting the ExprLiteral production.
	ExitExprLiteral(c *ExprLiteralContext)

	// ExitExprNot is called when exiting the ExprNot production.
	ExitExprNot(c *ExprNotContext)
}
