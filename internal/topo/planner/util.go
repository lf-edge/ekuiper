// Copyright 2021 EMQ Technologies Co., Ltd.
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

import "github.com/lf-edge/ekuiper/v2/pkg/ast"

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
		case *ast.BinaryExpr:
			if t.OP == ast.ARROW {
				hasMeta := false
				result, hasMeta = getFieldRef(n, result)
				if !hasMeta {
					result = append(result, t)
				}
				return hasMeta
			}
		}
		return true
	})
	return result
}

func getFieldRef(node ast.Node, result []ast.Expr) ([]ast.Expr, bool) {
	hasMeta := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.FieldRef:
			if t.IsColumn() {
				result = append(result, t)
			}
		case *ast.MetaRef:
			hasMeta = true
			return false
		}
		return true
	})
	return result, hasMeta
}
