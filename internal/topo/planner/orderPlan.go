package planner

import "github.com/lf-edge/ekuiper/pkg/ast"

type OrderPlan struct {
	baseLogicalPlan
	SortFields ast.SortFields
}

func (p OrderPlan) Init() *OrderPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *OrderPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.SortFields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
