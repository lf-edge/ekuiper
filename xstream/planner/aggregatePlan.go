package planner

import "github.com/emqx/kuiper/xsql"

type AggregatePlan struct {
	baseLogicalPlan
	dimensions xsql.Dimensions
	alias      xsql.Fields
}

func (p AggregatePlan) Init() *AggregatePlan {
	p.baseLogicalPlan.self = &p
	return &p
}
