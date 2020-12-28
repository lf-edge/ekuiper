package planner

import "github.com/emqx/kuiper/xsql"

type WindowPlan struct {
	baseLogicalPlan
	condition   xsql.Expr
	wtype       xsql.WindowType
	length      int
	interval    int //If interval is not set, it is equals to Length
	limit       int //If limit is not positive, there will be no limit
	isEventTime bool
}

func (p WindowPlan) Init() *WindowPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *WindowPlan) PushDownPredicate(condition xsql.Expr) (xsql.Expr, LogicalPlan) {
	if p.wtype == xsql.COUNT_WINDOW {
		return condition, p
	} else if p.isEventTime {
		// TODO event time filter, need event window op support
		//p.condition = combine(condition, p.condition)
		//// push nil condition won't return any
		//p.baseLogicalPlan.PushDownPredicate(nil)
		// return nil, p
		return condition, p
	} else {
		//Presume window condition are only one table related.
		// TODO window condition validation
		a := combine(condition, p.condition)
		p.condition, _ = p.baseLogicalPlan.PushDownPredicate(a)
		return nil, p
	}
}

func (p *WindowPlan) PruneColumns(fields []xsql.Expr) error {
	f := getFields(p.condition)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
