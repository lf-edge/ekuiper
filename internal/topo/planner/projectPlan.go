package planner

import "github.com/lf-edge/ekuiper/pkg/ast"

type ProjectPlan struct {
	baseLogicalPlan
	fields      ast.Fields
	isAggregate bool
	sendMeta    bool
}

func (p ProjectPlan) Init() *ProjectPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *ProjectPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.fields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
