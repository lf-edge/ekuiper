package ast

// IsAggregate check if an expression is aggregate with the binding alias info
func IsAggregate(expr Expr) (r bool) {
	WalkFunc(expr, func(n Node) bool {
		switch f := n.(type) {
		case *Call:
			if ok := FuncFinderSingleton().IsAggFunc(f); ok {
				r = true
				return false
			}
		case *FieldRef:
			if f.IsAggregate() {
				r = true
				return false
			}
		}
		return true
	})
	return
}

func IsAggStatement(stmt *SelectStatement) bool {
	if stmt.Dimensions != nil {
		ds := stmt.Dimensions.GetGroups()
		if ds != nil && len(ds) > 0 {
			return true
		}
	}
	r := false
	WalkFunc(stmt.Fields, func(n Node) bool {
		switch f := n.(type) {
		case *Call:
			if ok := FuncFinderSingleton().IsAggFunc(f); ok {
				r = true
				return false
			}
		}
		return true
	})
	return r
}

func HasAggFuncs(node Node) bool {
	if node == nil {
		return false
	}
	var r = false
	WalkFunc(node, func(n Node) bool {
		if f, ok := n.(*Call); ok {
			if ok := FuncFinderSingleton().IsAggFunc(f); ok {
				r = true
				return false
			}
		}
		return true
	})
	return r
}
