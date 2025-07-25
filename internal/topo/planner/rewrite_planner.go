// Copyright 2025 EMQ Technologies Co., Ltd.
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

package planner

import (
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type rewriteResult struct {
	windowFuncFields     []*ast.Field
	incAggFields         []*ast.Field
	dsColAliasMapping    map[ast.StreamName]map[string]string
	aggFuncsFieldInWhere []*ast.Field
}

// rewrite stmt will do following things:
// 1. extract and rewrite the window function
// 2. extract and rewrite the aggregation function
func rewriteStmt(stmt *ast.SelectStatement, opt *def.RuleOption) rewriteResult {
	result := rewriteResult{}
	result.windowFuncFields = extractWindowFuncFields(stmt)
	result.incAggFields = rewriteIfIncAggStmt(stmt, opt)
	result.dsColAliasMapping = rewriteIfPushdownAlias(stmt, opt)
	result.aggFuncsFieldInWhere = rewriteAggFunctionInWhere(stmt, opt)
	return result
}

// extract agg function from filter condition and rewrite with bypass fields
func rewriteAggFunctionInWhere(stmt *ast.SelectStatement, opt *def.RuleOption) []*ast.Field {
	if !opt.PlanOptimizeStrategy.AllowAggFuncInWhere {
		return nil
	}
	aggFuncsFieldInWhere := make([]*ast.Field, 0)
	var index int
	ast.WalkFunc(stmt.Condition, func(node ast.Node) bool {
		switch aggFunc := node.(type) {
		case *ast.Call:
			if aggFunc.FuncType == ast.FuncTypeAgg {
				newAggFunc := &ast.Call{
					Name:     aggFunc.Name,
					FuncType: aggFunc.FuncType,
					Args:     aggFunc.Args,
					FuncId:   aggFunc.FuncId,
				}
				name := fmt.Sprintf("agg_ref_%v", index)
				newField := &ast.Field{
					Name: name,
					Expr: newAggFunc,
				}
				aggFuncsFieldInWhere = append(aggFuncsFieldInWhere, newField)
				newFieldRef := &ast.FieldRef{
					StreamName: ast.DefaultStream,
					Name:       name,
				}
				rewriteIntoBypass(newFieldRef, aggFunc)
			}
		}
		return true
	})
	return aggFuncsFieldInWhere
}
