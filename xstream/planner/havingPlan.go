package planner

import "github.com/emqx/kuiper/xsql"

type HavingPlan struct {
	baseLogicalPlan
	condition xsql.Expr
}

func (p HavingPlan) Init() *HavingPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *HavingPlan) PruneColumns(fields []xsql.Expr) error {
	f := getFields(p.condition)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
