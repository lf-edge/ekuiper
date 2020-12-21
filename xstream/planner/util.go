package planner

import "github.com/emqx/kuiper/xsql"

func getRefSources(node xsql.Node) []string {
	result := make(map[string]bool)
	keys := make([]string, 0, len(result))
	if node == nil {
		return keys
	}
	xsql.WalkFunc(node, func(n xsql.Node) {
		if f, ok := n.(*xsql.FieldRef); ok {
			result[string(f.StreamName)] = true
		}
	})
	for k := range result {
		keys = append(keys, k)
	}
	return keys
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
