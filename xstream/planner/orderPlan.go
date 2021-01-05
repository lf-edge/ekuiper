package planner

import "github.com/emqx/kuiper/xsql"

type OrderPlan struct {
	baseLogicalPlan
	SortFields xsql.SortFields
}

func (p OrderPlan) Init() *OrderPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *OrderPlan) PruneColumns(fields []xsql.Expr) error {
	f := getFields(p.SortFields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
