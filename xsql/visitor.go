package xsql

type ExpressionVisitor interface {
	Visit(Node) Visitor

	VisitBinaryExpr(*BinaryExpr)
	VisitFieldRef(*FieldRef)
	VisitIntegerLiteral(*IntegerLiteral)
}

type ExpressionVisitorAdaptor struct {
}

func (eva *ExpressionVisitorAdaptor) DoVisit(v ExpressionVisitor, expr Node) {
	switch n := expr.(type) {
	case *BinaryExpr:
		v.VisitBinaryExpr(n)
	case *FieldRef:
		v.VisitFieldRef(n)
	case *IntegerLiteral:
		v.VisitIntegerLiteral(n)
	}
}

func (eva *ExpressionVisitorAdaptor) Visit(expr Node) Visitor {
	return nil
}

func (eva *ExpressionVisitorAdaptor) VisitBinaryExpr(expr *BinaryExpr) {
	Walk(eva, expr.LHS)
	Walk(eva, expr.RHS)
}

func (eva *ExpressionVisitorAdaptor) VisitFieldRef(expr *FieldRef) {
	Walk(eva, expr)
}

func (eva *ExpressionVisitorAdaptor) visitIntegerLiteral(expr *FieldRef) {

}
