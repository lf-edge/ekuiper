package planner

import "github.com/emqx/kuiper/pkg/ast"

type AggregatePlan struct {
	baseLogicalPlan
	dimensions ast.Dimensions
}

func (p AggregatePlan) Init() *AggregatePlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *AggregatePlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.dimensions)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
