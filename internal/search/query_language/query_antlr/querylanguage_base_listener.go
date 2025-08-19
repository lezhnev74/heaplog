// Code generated from /home/dmitry/Code/go/src/heaplog/heaplog_2024/internal/search/query_language/QueryLanguage.g4 by ANTLR 4.13.2. DO NOT EDIT.

package query_antlr // QueryLanguage
import "github.com/antlr4-go/antlr/v4"

// BaseQueryLanguageListener is a complete listener for a parse tree produced by QueryLanguageParser.
type BaseQueryLanguageListener struct{}

var _ QueryLanguageListener = &BaseQueryLanguageListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseQueryLanguageListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseQueryLanguageListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseQueryLanguageListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseQueryLanguageListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseQueryLanguageListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseQueryLanguageListener) ExitQuery(ctx *QueryContext) {}

// EnterExprAnd is called when production ExprAnd is entered.
func (s *BaseQueryLanguageListener) EnterExprAnd(ctx *ExprAndContext) {}

// ExitExprAnd is called when production ExprAnd is exited.
func (s *BaseQueryLanguageListener) ExitExprAnd(ctx *ExprAndContext) {}

// EnterExprGroup is called when production ExprGroup is entered.
func (s *BaseQueryLanguageListener) EnterExprGroup(ctx *ExprGroupContext) {}

// ExitExprGroup is called when production ExprGroup is exited.
func (s *BaseQueryLanguageListener) ExitExprGroup(ctx *ExprGroupContext) {}

// EnterExprRELiteralCS is called when production ExprRELiteralCS is entered.
func (s *BaseQueryLanguageListener) EnterExprRELiteralCS(ctx *ExprRELiteralCSContext) {}

// ExitExprRELiteralCS is called when production ExprRELiteralCS is exited.
func (s *BaseQueryLanguageListener) ExitExprRELiteralCS(ctx *ExprRELiteralCSContext) {}

// EnterExprOr is called when production ExprOr is entered.
func (s *BaseQueryLanguageListener) EnterExprOr(ctx *ExprOrContext) {}

// ExitExprOr is called when production ExprOr is exited.
func (s *BaseQueryLanguageListener) ExitExprOr(ctx *ExprOrContext) {}

// EnterExprRELiteral is called when production ExprRELiteral is entered.
func (s *BaseQueryLanguageListener) EnterExprRELiteral(ctx *ExprRELiteralContext) {}

// ExitExprRELiteral is called when production ExprRELiteral is exited.
func (s *BaseQueryLanguageListener) ExitExprRELiteral(ctx *ExprRELiteralContext) {}

// EnterExprLiteral is called when production ExprLiteral is entered.
func (s *BaseQueryLanguageListener) EnterExprLiteral(ctx *ExprLiteralContext) {}

// ExitExprLiteral is called when production ExprLiteral is exited.
func (s *BaseQueryLanguageListener) ExitExprLiteral(ctx *ExprLiteralContext) {}

// EnterExprNot is called when production ExprNot is entered.
func (s *BaseQueryLanguageListener) EnterExprNot(ctx *ExprNotContext) {}

// ExitExprNot is called when production ExprNot is exited.
func (s *BaseQueryLanguageListener) ExitExprNot(ctx *ExprNotContext) {}
