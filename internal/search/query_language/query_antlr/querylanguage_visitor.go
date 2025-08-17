// Code generated from /home/dmitry/Code/go/src/heaplog/heaplog_2024/internal/search/query_language/QueryLanguage.g4 by ANTLR 4.13.2. DO NOT EDIT.

package query_antlr // QueryLanguage
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by QueryLanguageParser.
type QueryLanguageVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by QueryLanguageParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by QueryLanguageParser#ExprAnd.
	VisitExprAnd(ctx *ExprAndContext) interface{}

	// Visit a parse tree produced by QueryLanguageParser#ExprGroup.
	VisitExprGroup(ctx *ExprGroupContext) interface{}

	// Visit a parse tree produced by QueryLanguageParser#ExprOr.
	VisitExprOr(ctx *ExprOrContext) interface{}

	// Visit a parse tree produced by QueryLanguageParser#ExprRELiteral.
	VisitExprRELiteral(ctx *ExprRELiteralContext) interface{}

	// Visit a parse tree produced by QueryLanguageParser#ExprLiteral.
	VisitExprLiteral(ctx *ExprLiteralContext) interface{}

	// Visit a parse tree produced by QueryLanguageParser#ExprNot.
	VisitExprNot(ctx *ExprNotContext) interface{}
}
