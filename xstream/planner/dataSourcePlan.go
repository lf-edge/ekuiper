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
	if condition != nil {
		// Add a filter plan for children
		f := FilterPlan{
			condition: condition,
		}.Init()
		f.SetChildren([]LogicalPlan{p})
		return nil, f
	}
	return nil, p
}
