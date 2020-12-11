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
