package planner

import "github.com/emqx/kuiper/xsql"

type DataSourcePlan struct {
	baseLogicalPlan
	name       string
	isWildCard bool
	needMeta   bool
	// if is wildCard, leave it empty
	fields     xsql.Fields
	metaFields xsql.Fields
	alias      xsql.Fields
}

func (p DataSourcePlan) Init() *DataSourcePlan {
	p.baseLogicalPlan.self = &p
	return &p
}

// Presume no children for data source
func (p *DataSourcePlan) PushDownPredicate(condition xsql.Expr) (xsql.Expr, LogicalPlan) {
	owned, other := p.extract(condition)
	if owned != nil {
		// Add a filter plan for children
		f := FilterPlan{
			condition: owned,
		}.Init()
		f.SetChildren([]LogicalPlan{p})
		return other, f
	}
	return other, p
}

func (p *DataSourcePlan) extract(expr xsql.Expr) (xsql.Expr, xsql.Expr) {
	s := getRefSources(expr)
	switch len(s) {
	case 0:
		return expr, nil
	case 1:
		if s[0] == p.name {
			return expr, nil
		} else {
			return nil, expr
		}
	default:
		if be, ok := expr.(*xsql.BinaryExpr); ok && be.OP == xsql.AND {
			ul, pl := p.extract(be.LHS)
			ur, pr := p.extract(be.RHS)
			owned := combine(ul, ur)
			other := combine(pl, pr)
			return owned, other
		}
		return nil, expr
	}
}
