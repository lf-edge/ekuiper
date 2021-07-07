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
