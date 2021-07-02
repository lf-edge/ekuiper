package xsql

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

// Validate validate select statement without context.
// This is the pre-validation. In planner, there will be a more comprehensive validation after binding
func Validate(stmt *ast.SelectStatement) error {
	if ast.HasAggFuncs(stmt.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}

	for _, d := range stmt.Dimensions {
		if ast.HasAggFuncs(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}
	return nil
}
