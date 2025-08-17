// Code generated from /home/dmitry/Code/go/src/heaplog/heaplog_2024/internal/search/query_language/QueryLanguage.g4 by ANTLR 4.13.2. DO NOT EDIT.

package query_antlr // QueryLanguage
import "github.com/antlr4-go/antlr/v4"

type BaseQueryLanguageVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseQueryLanguageVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseQueryLanguageVisitor) VisitExprAnd(ctx *ExprAndContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseQueryLanguageVisitor) VisitExprGroup(ctx *ExprGroupContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseQueryLanguageVisitor) VisitExprOr(ctx *ExprOrContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseQueryLanguageVisitor) VisitExprRELiteral(ctx *ExprRELiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseQueryLanguageVisitor) VisitExprLiteral(ctx *ExprLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseQueryLanguageVisitor) VisitExprNot(ctx *ExprNotContext) interface{} {
	return v.VisitChildren(ctx)
}
