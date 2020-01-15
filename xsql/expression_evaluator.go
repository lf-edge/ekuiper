package xsql

import (
	"github.com/buger/jsonparser"
	"github.com/emqx/kuiper/common"
	"github.com/golang-collections/collections/stack"
)

type ExpressionEvaluator struct {
	*ExpressionVisitorAdaptor
	tuple    []byte
	operands *stack.Stack
}

func newExpressionEvaluator(tuple []byte) *ExpressionEvaluator {
	ee := &ExpressionEvaluator{tuple: tuple}
	ee.operands = stack.New()
	return ee
}

func (ee *ExpressionEvaluator) Visit(expr Node) Visitor {
	ee.DoVisit(ee, expr)
	return nil
}

func (ee *ExpressionEvaluator) VisitBinaryExpr(expr *BinaryExpr) {
	Walk(ee, expr.LHS)
	Walk(ee, expr.RHS)
	ee.operands.Push(expr.OP)
}

func (ee *ExpressionEvaluator) VisitFieldRef(expr *FieldRef) {
	//TODO GetXXX, how to determine the type
	if fv, err := jsonparser.GetInt(ee.tuple, expr.Name); err != nil {
		common.Log.Printf("Cannot find value in %s with field name %s.\n", string(ee.tuple), expr.Name)
	} else {
		ee.operands.Push(fv)
	}
}

func (ee *ExpressionEvaluator) VisitIntegerLiteral(expr *IntegerLiteral) {
	ee.operands.Push(expr.Val)
}
