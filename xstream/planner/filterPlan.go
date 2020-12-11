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
	// if has child, try to move pushable condition out
	up, pp := extractCondition(a)

	rest, _ := p.baseLogicalPlan.PushDownPredicate(pp)

	up = combine(up, rest)
	if up != nil {
		p.condition = up
		return nil, p
	} else if len(p.children) == 1 {
		// eliminate this filter
		return nil, p.children[0]
	} else {
		return nil, p
	}
}

// Return the unpushable condition and pushable condition
func extractCondition(condition xsql.Expr) (unpushable xsql.Expr, pushable xsql.Expr) {
	s := GetRefSources(condition)
	if len(s) < 2 {
		pushable = condition
		return
	} else {
		if be, ok := condition.(*xsql.BinaryExpr); ok && be.OP == xsql.AND {
			ul, pl := extractCondition(be.LHS)
			ur, pr := extractCondition(be.RHS)
			unpushable = combine(ul, ur)
			pushable = combine(pl, pr)
			return
		}
	}
	//default case: all condition are unpushable
	return condition, nil
}

func combine(l xsql.Expr, r xsql.Expr) xsql.Expr {
	if l != nil && r != nil {
		return &xsql.BinaryExpr{
			OP:  xsql.AND,
			LHS: l,
			RHS: r,
		}
	} else if l != nil {
		return l
	} else {
		return r
	}
}
