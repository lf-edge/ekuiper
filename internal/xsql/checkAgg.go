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

package xsql

import (
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

// IsAggregate check if an expression is aggregate with the binding alias info
func IsAggregate(expr ast.Expr) (r bool) {
	ast.WalkFunc(expr, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if ok := function.IsAggFunc(f.Name); ok {
				r = true
				return false
			}
		case *ast.FieldRef:
			// lazy calculate
			if getOrCalculateAgg(f) {
				r = true
				return false
			}
		}
		return true
	})
	return
}

func getOrCalculateAgg(f *ast.FieldRef) bool {
	if f.IsAlias() {
		p := f.IsAggregate
		if p == nil {
			tr := IsAggregate(f.Expression)
			p = &tr
			f.IsAggregate = p
		}
		return *p
	}
	return false
}

func IsAggStatement(stmt *ast.SelectStatement) bool {
	if stmt.Dimensions != nil {
		ds := stmt.Dimensions.GetGroups()
		if ds != nil && len(ds) > 0 {
			return true
		}
	}
	r := false
	ast.WalkFunc(stmt.Fields, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if ok := function.IsAggFunc(f.Name); ok {
				r = true
				return false
			}
		}
		return true
	})
	return r
}

func HasAggFuncs(node ast.Node) bool {
	if node == nil {
		return false
	}
	var r = false
	ast.WalkFunc(node, func(n ast.Node) bool {
		if f, ok := n.(*ast.Call); ok {
			if ok := function.IsAggFunc(f.Name); ok {
				r = true
				return false
			}
		}
		return true
	})
	return r
}
