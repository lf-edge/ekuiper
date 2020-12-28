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

func (p *JoinPlan) PushDownPredicate(condition xsql.Expr) (xsql.Expr, LogicalPlan) {
	//TODO multiple join support
	//Assume only one join
	j := p.joins[0]
	switch j.JoinType {
	case xsql.INNER_JOIN:
		a := combine(condition, j.Expr)
		multipleSourcesCondition, singleSourceCondition := extractCondition(a)
		rest, _ := p.baseLogicalPlan.PushDownPredicate(singleSourceCondition)
		j.Expr = combine(multipleSourcesCondition, rest) //always swallow all conditions
		p.joins[0] = j
		return nil, p
	default: //TODO fine grain handling for left/right join
		multipleSourcesCondition, singleSourceCondition := extractCondition(condition)
		rest, _ := p.baseLogicalPlan.PushDownPredicate(singleSourceCondition)
		// never swallow anything
		return combine(multipleSourcesCondition, rest), p
	}
}

// Return the unpushable condition and pushable condition
func extractCondition(condition xsql.Expr) (unpushable xsql.Expr, pushable xsql.Expr) {
	s := getRefSources(condition)
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

func (p *JoinPlan) PruneColumns(fields []xsql.Expr) error {
	f := getFields(p.joins)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
