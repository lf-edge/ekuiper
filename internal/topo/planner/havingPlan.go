package planner

import "github.com/emqx/kuiper/pkg/ast"

type HavingPlan struct {
	baseLogicalPlan
	condition ast.Expr
}

func (p HavingPlan) Init() *HavingPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *HavingPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.condition)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
