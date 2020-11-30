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

	return nil
}
