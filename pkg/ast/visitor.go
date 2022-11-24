// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

import (
	"reflect"
)

type Visitor interface {
	Visit(Node) bool
}

func Walk(v Visitor, node Node) {
	if node == nil || reflect.ValueOf(node).IsNil() {
		return
	}

	if !v.Visit(node) {
		return
	}

	switch n := node.(type) {
	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Sources)
		Walk(v, n.Joins)
		Walk(v, n.Condition)
		Walk(v, n.Dimensions)
		Walk(v, n.Having)
		Walk(v, n.SortFields)

	case Fields:
		for _, f := range n {
			Walk(v, &f)
		}

	case *Field:
		Walk(v, n.Expr)
		if fr, ok := n.Expr.(*FieldRef); ok && fr.IsAlias() {
			Walk(v, fr.Expression)
		}

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	//case *Table:

	case Joins:
		for _, s := range n {
			Walk(v, &s)
		}

	case *Join:
		Walk(v, n.Expr)

	case Dimensions:
		Walk(v, n.GetWindow())
		for _, dimension := range n.GetGroups() {
			Walk(v, dimension.Expr)
		}

	case *Window:
		Walk(v, n.Length)
		Walk(v, n.Interval)
		Walk(v, n.Filter)

	case SortFields:
		for _, sf := range n {
			Walk(v, sf.FieldExpr)
		}

	//case *SortField:

	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

		if n.Partition != nil {
			for _, expr := range n.Partition.Exprs {
				Walk(v, expr)
			}
		}

		if n.WhenExpr != nil {
			Walk(v, n.WhenExpr)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *CaseExpr:
		Walk(v, n.Value)
		for _, w := range n.WhenClauses {
			Walk(v, w)
		}
		Walk(v, n.ElseClause)

	case *WhenClause:
		Walk(v, n.Expr)
		Walk(v, n.Result)

	case *IndexExpr:
		Walk(v, n.Index)

	case *ColFuncField:
		Walk(v, n.Expr)

	case *ValueSetExpr:
		for _, l := range n.LiteralExprs {
			Walk(v, l)
		}
		Walk(v, n.ArrayExpr)
	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node) bool) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node) bool

func (fn walkFuncVisitor) Visit(n Node) bool { return fn(n) }
