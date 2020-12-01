package xsql

import "fmt"

func Validate(stmt *SelectStatement) error {
	if HasAggFuncs(stmt.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}

	if HasNoAggFuncs(stmt.Having) {
		return fmt.Errorf("Not allowed to call none-aggregate functions in HAVING clause.")
	}

	for _, d := range stmt.Dimensions {
		if HasAggFuncs(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}

	//Cannot GROUP BY alias fields with aggregate funcs
	//	for _, d := range stmt.Dimensions {
	//		if f, ok := d.Expr.(*FieldRef); ok {
	//			for _, f1 := range stmt.Fields {
	//				if f.Name == f1.Name || f.Name == f1.AName {
	//					if HasAggFuncs(f1.Expr) {
	//						return fmt.Errorf("Cannot group on %s.", f.Name)
	//					}
	//					break
	//				}
	//			}
	//		} else {
	//			return fmt.Errorf("Invalid use of group function")
	//		}
	//
	//	}
	return nil
}
