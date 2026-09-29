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

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type AggFuncPlan struct {
	baseLogicalPlan
	aggFields []*ast.Field
}

func (p AggFuncPlan) Init() *AggFuncPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(AggFunc)
	return &p
}

// PushDownPredicate keeps the parts of the condition that read the aggregate
// results computed by this plan, and pushes the rest down to its children.
func (p *AggFuncPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	unpushable, pushable := p.extractAggCondition(condition)
	rest, _ := p.baseLogicalPlan.PushDownPredicate(pushable)
	return combine(unpushable, rest), p
}

// extractAggCondition splits the AND parts of the condition into the ones that
// reference an aggregate field of this plan (unpushable) and the others.
func (p *AggFuncPlan) extractAggCondition(condition ast.Expr) (unpushable ast.Expr, pushable ast.Expr) {
	if condition == nil {
		return nil, nil
	}
	if be, ok := condition.(*ast.BinaryExpr); ok && be.OP == ast.AND {
		ul, pl := p.extractAggCondition(be.LHS)
		ur, pr := p.extractAggCondition(be.RHS)
		return combine(ul, ur), combine(pl, pr)
	}
	if p.refAggFields(condition) {
		return condition, nil
	}
	return nil, condition
}

func (p *AggFuncPlan) refAggFields(expr ast.Expr) bool {
	found := false
	ast.WalkFunc(expr, func(n ast.Node) bool {
		if f, ok := n.(*ast.FieldRef); ok && f.StreamName == ast.DefaultStream {
			for _, aggField := range p.aggFields {
				if aggField.Name == f.Name {
					found = true
					return false
				}
			}
		}
		return !found
	})
	return found
}

func (p *AggFuncPlan) BuildExplainInfo() {
	info := ""
	if len(p.aggFields) > 0 {
		info += "aggFuncs:["
		for _, aggField := range p.aggFields {
			info += fmt.Sprintf("%v:%s", aggField.Name, aggField.Expr.String())
		}
		info += "]"
	}
	p.baseLogicalPlan.ExplainInfo.Info = info
}
