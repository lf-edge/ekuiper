// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xsql

import (
	"fmt"

	"github.com/lf-edge/ekuiper/pkg/ast"
)

// Validate validate select statement without context.
// This is the pre-validation. In planner, there will be a more comprehensive validation after binding
func Validate(stmt *ast.SelectStatement) error {
	if HasAggFuncs(stmt.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}

	for _, d := range stmt.Dimensions {
		if HasAggFuncs(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}

	if err := validateSRFNestedForbidden("select", stmt.Fields); err != nil {
		return err
	}
	if err := validateMultiSRFForbidden("select", stmt.Fields); err != nil {
		return err
	}
	return validateSRFForbidden(stmt)
}

func validateSRFNestedForbidden(clause string, node ast.Node) error {
	if isSRFNested(node) {
		return fmt.Errorf("%s clause shouldn't has nested set-returning-functions", clause)
	}
	return nil
}

func validateMultiSRFForbidden(clause string, node ast.Node) error {
	firstSRF := false
	nextSRF := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if f.FuncType == ast.FuncTypeSrf {
				if !firstSRF {
					firstSRF = true
				} else {
					nextSRF = true
					return false
				}
			}
		}
		return true
	})
	if nextSRF {
		return fmt.Errorf("%s clause shouldn't has multi set-returning-functions", clause)
	}
	return nil
}

func validateSRFForbidden(node ast.Node) error {
	if isSRFExists(node) {
		return fmt.Errorf("select statement shouldn't has srf except fields")
	}
	return nil
}

func isSRFNested(node ast.Node) bool {
	srfNested := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			for _, arg := range f.Args {
				exists := isSRFExists(arg)
				if exists {
					srfNested = true
					return false
				}
			}
			return true
		}
		return true
	})
	return srfNested
}

func isSRFExists(node ast.Node) bool {
	exists := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		// skip checking Fields
		case ast.Fields:
			return false
		case *ast.Call:
			if f.FuncType == ast.FuncTypeSrf {
				exists = true
				return false
			}
		}
		return true
	})
	return exists
}

func validateFields(stmt *ast.SelectStatement) {
	streamName := getStreamNames(stmt)
	for i, field := range stmt.Fields {
		stmt.Fields[i].Expr = validateExpr(field.Expr, streamName)
	}
	for i, join := range stmt.Joins {
		stmt.Joins[i].Expr = validateExpr(join.Expr, streamName)
	}
}

func validateExpr(expr ast.Expr, streamName []string) ast.Expr {
	switch expr.(type) {
	case *ast.BinaryExpr:
		e := expr.(*ast.BinaryExpr)
		exp := ast.BinaryExpr{}
		exp.OP = e.OP
		if e.OP == ast.DOT {
			exp.OP = ast.ARROW
		}
		exp.RHS = validateExpr(e.RHS, streamName)
		exp.LHS = validateExpr(e.LHS, streamName)
		return &exp
	case *ast.FieldRef:
		sn := string(expr.(*ast.FieldRef).StreamName)
		if sn != string(ast.DefaultStream) && !contains(streamName, sn) {
			return &ast.BinaryExpr{OP: ast.ARROW, LHS: &ast.FieldRef{Name: string(expr.(*ast.FieldRef).StreamName), StreamName: ast.DefaultStream}, RHS: &ast.JsonFieldRef{Name: expr.(*ast.FieldRef).Name}}
		}
		return expr
	case *ast.MetaRef:
		sn := string(expr.(*ast.MetaRef).StreamName)
		if sn != string(ast.DefaultStream) && !contains(streamName, sn) {
			return &ast.BinaryExpr{OP: ast.ARROW, LHS: &ast.MetaRef{Name: string(expr.(*ast.MetaRef).StreamName), StreamName: ast.DefaultStream}, RHS: &ast.JsonFieldRef{Name: expr.(*ast.MetaRef).Name}}
		}
		return expr
	case *ast.BetweenExpr:
		e := expr.(*ast.BetweenExpr)
		e.Higher = validateExpr(e.Higher, streamName)
		e.Lower = validateExpr(e.Lower, streamName)
		return e
	case *ast.ColonExpr:
		e := expr.(*ast.ColonExpr)
		e.Start = validateExpr(e.Start, streamName)
		e.End = validateExpr(e.End, streamName)
		return e
	case *ast.CaseExpr:
		e := expr.(*ast.CaseExpr)
		e.Value = validateExpr(e.Value, streamName)
		e.ElseClause = validateExpr(e.ElseClause, streamName)
		for i, when := range e.WhenClauses {
			e.WhenClauses[i].Expr = validateExpr(when.Expr, streamName)
			e.WhenClauses[i].Result = validateExpr(when.Result, streamName)
		}
		return e
	case *ast.Call:
		e := expr.(*ast.Call)
		for i, arg := range e.Args {
			e.Args[i] = validateExpr(arg, streamName)
		}
		if e.Partition != nil {
			for i, p := range e.Partition.Exprs {
				e.Partition.Exprs[i] = validateExpr(p, streamName)
			}
		}
		if e.WhenExpr != nil {
			e.WhenExpr = validateExpr(e.WhenExpr, streamName)
		}
		return e
	case *ast.ParenExpr:
		e := expr.(*ast.ParenExpr)
		e.Expr = validateExpr(e.Expr, streamName)
		return e
	case *ast.IndexExpr:
		e := expr.(*ast.IndexExpr)
		e.Index = validateExpr(e.Index, streamName)
		return e
	case *ast.ColFuncField:
		e := expr.(*ast.ColFuncField)
		e.Expr = validateExpr(e.Expr, streamName)
		return e
	case *ast.LikePattern:
		e := expr.(*ast.LikePattern)
		e.Expr = validateExpr(e.Expr, streamName)
		return e
	case *ast.ValueSetExpr:
		e := expr.(*ast.ValueSetExpr)
		e.ArrayExpr = validateExpr(e.ArrayExpr, streamName)
		for i, v := range e.LiteralExprs {
			e.LiteralExprs[i] = validateExpr(v, streamName)
		}
		return e
	case *ast.PartitionExpr:
		e := expr.(*ast.PartitionExpr)
		for i, v := range e.Exprs {
			e.Exprs[i] = validateExpr(v, streamName)
		}
		return e
	case *ast.SortField:
		e := expr.(*ast.SortField)
		e.Expr = validateExpr(e.Expr, streamName)
		e.FieldExpr = validateExpr(e.FieldExpr, streamName)
		return e
	case *ast.WhenClause:
		e := expr.(*ast.WhenClause)
		e.Expr = validateExpr(e.Expr, streamName)
		e.Result = validateExpr(e.Result, streamName)
		return e
	case *ast.Window:
		e := expr.(*ast.Window)
		e.Filter = validateExpr(e.Filter, streamName)
		e.Expr = validateExpr(e.Expr, streamName)
		return e
	default:
		return expr
	}
}

func contains(streamName []string, name string) bool {
	for _, s := range streamName {
		if s == name {
			return true
		}
	}
	return false
}

func getStreamNames(stmt *ast.SelectStatement) (result []string) {
	if stmt == nil {
		return nil
	}

	for _, source := range stmt.Sources {
		if s, ok := source.(*ast.Table); ok {
			result = append(result, s.Name)
			if s.Alias != "" {
				result = append(result, s.Alias)
			}
		}
	}

	for _, join := range stmt.Joins {
		result = append(result, join.Name)
		if join.Alias != "" {
			result = append(result, join.Alias)
		}
	}
	return
}
