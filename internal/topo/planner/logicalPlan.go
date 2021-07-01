package planner

import "github.com/emqx/kuiper/pkg/ast"

type LogicalPlan interface {
	Children() []LogicalPlan
	SetChildren(children []LogicalPlan)
	// PushDownPredicate pushes down the filter in the filter/where/on/having clauses as deeply as possible.
	// It will accept a condition that is an expression slice, and return the expressions that can't be pushed.
	// It also return the new tree of plan as it can possibly change the tree
	PushDownPredicate(ast.Expr) (ast.Expr, LogicalPlan)
	// Prune the unused columns in the data source level, by pushing all needed columns down
	PruneColumns(fields []ast.Expr) error
}

type baseLogicalPlan struct {
	children []LogicalPlan
	// Can be used to return the derived instance from the base type
	self LogicalPlan
}

func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

func (p *baseLogicalPlan) SetChildren(children []LogicalPlan) {
	p.children = children
}

// By default, push down the predicate to the first child instead of the children
// as most plan cannot have multiple children
func (p *baseLogicalPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	if len(p.children) == 0 {
		return condition, p.self
	}
	rest := condition
	for i, child := range p.children {
		var newChild LogicalPlan
		rest, newChild = child.PushDownPredicate(rest)
		p.children[i] = newChild
	}
	return rest, p.self
}

func (p *baseLogicalPlan) PruneColumns(fields []ast.Expr) error {
	for _, child := range p.children {
		err := child.PruneColumns(fields)
		if err != nil {
			return err
		}
	}
	return nil
}
