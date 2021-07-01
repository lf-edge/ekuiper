package planner

import "github.com/emqx/kuiper/pkg/ast"

func getRefSources(node ast.Node) ([]ast.StreamName, bool) {
	result := make(map[ast.StreamName]bool)
	keys := make([]ast.StreamName, 0, len(result))
	if node == nil {
		return keys, false
	}
	hasDefault := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		if f, ok := n.(*ast.FieldRef); ok {
			for _, sn := range f.RefSources() {
				if sn == ast.DefaultStream {
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

func combine(l ast.Expr, r ast.Expr) ast.Expr {
	if l != nil && r != nil {
		return &ast.BinaryExpr{
			OP:  ast.AND,
			LHS: l,
			RHS: r,
		}
	} else if l != nil {
		return l
	} else {
		return r
	}
}

func getFields(node ast.Node) []ast.Expr {
	result := make([]ast.Expr, 0)
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.FieldRef:
			if t.IsColumn() {
				result = append(result, t)
			}
		case *ast.Wildcard:
			result = append(result, t)
		case *ast.MetaRef:
			if t.StreamName != "" {
				result = append(result, t)
			}
		case *ast.SortField:
			result = append(result, t)
		}
		return true
	})
	return result
}
