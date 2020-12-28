package planner

import "github.com/emqx/kuiper/xsql"

type ProjectPlan struct {
	baseLogicalPlan
	fields      xsql.Fields
	isAggregate bool
	sendMeta    bool
}

func (p ProjectPlan) Init() *ProjectPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *ProjectPlan) PruneColumns(fields []xsql.Expr) error {
	f := getFields(p.fields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
