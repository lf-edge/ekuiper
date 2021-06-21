package planner

import "github.com/emqx/kuiper/xsql"

func getRefSources(node xsql.Node) ([]xsql.StreamName, bool) {
	result := make(map[xsql.StreamName]bool)
	keys := make([]xsql.StreamName, 0, len(result))
	if node == nil {
		return keys, false
	}
	hasDefault := false
	xsql.WalkFunc(node, func(n xsql.Node) bool {
		if f, ok := n.(*xsql.FieldRef); ok {
			for _, sn := range f.RefSources() {
				if sn == xsql.DefaultStream {
					hasDefault = true
				}
				result[sn] = true
			}
			return false
		}
		return true
	})
	for k := range result {
		keys = append(keys, k)
	}
	return keys, hasDefault
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

func getFields(node xsql.Node) []xsql.Expr {
	result := make([]xsql.Expr, 0)
	xsql.WalkFunc(node, func(n xsql.Node) bool {
		switch t := n.(type) {
		case *xsql.FieldRef:
			if t.IsColumn() {
				result = append(result, t)
			}
		case *xsql.Wildcard:
			result = append(result, t)
		case *xsql.MetaRef:
			if t.StreamName != "" {
				result = append(result, t)
			}
		case *xsql.SortField:
			result = append(result, t)
		}
		return true
	})
	return result
}
