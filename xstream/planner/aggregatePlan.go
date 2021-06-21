package planner

import "github.com/emqx/kuiper/xsql"

type AggregatePlan struct {
	baseLogicalPlan
	dimensions xsql.Dimensions
}

func (p AggregatePlan) Init() *AggregatePlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *AggregatePlan) PruneColumns(fields []xsql.Expr) error {
	f := getFields(p.dimensions)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
