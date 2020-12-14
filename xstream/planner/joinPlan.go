package planner

import "github.com/emqx/kuiper/xsql"

type JoinPlan struct {
	baseLogicalPlan
	from  *xsql.Table
	joins xsql.Joins
}

func (p JoinPlan) Init() *JoinPlan {
	p.baseLogicalPlan.self = &p
	return &p
}
