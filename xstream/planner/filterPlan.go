package planner

import "github.com/emqx/kuiper/xsql"

type FilterPlan struct {
	baseLogicalPlan
	condition xsql.Expr
}

func (p FilterPlan) Init() *FilterPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *FilterPlan) PushDownPredicate(condition xsql.Expr) (xsql.Expr, LogicalPlan) {
	// if no child, swallow all conditions
	a := combine(condition, p.condition)
	if len(p.children) == 0 {
		p.condition = a
		return nil, p
	}

	rest, _ := p.baseLogicalPlan.PushDownPredicate(a)

	if rest != nil {
		p.condition = rest
		return nil, p
	} else if len(p.children) == 1 {
		// eliminate this filter
		return nil, p.children[0]
	} else {
		return nil, p
	}
}
