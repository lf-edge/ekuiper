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

import "github.com/lf-edge/ekuiper/pkg/ast"

type JoinPlan struct {
	baseLogicalPlan
	from  *ast.Table
	joins ast.Joins
}

func (p JoinPlan) Init() *JoinPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p JoinPlan) BuildExplainInfo(id int64) {
	info := ""
	p.baseLogicalPlan.ExplainInfo.Id = id
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p *JoinPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	// TODO multiple join support
	// Assume only one join
	j := p.joins[0]
	switch j.JoinType {
	case ast.INNER_JOIN:
		a := combine(condition, j.Expr)
		multipleSourcesCondition, singleSourceCondition := extractCondition(a)
		rest, _ := p.baseLogicalPlan.PushDownPredicate(singleSourceCondition)
		j.Expr = combine(multipleSourcesCondition, rest) // always swallow all conditions
		p.joins[0] = j
		return nil, p
	default: // TODO fine grain handling for left/right join
		multipleSourcesCondition, singleSourceCondition := extractCondition(condition)
		rest, _ := p.baseLogicalPlan.PushDownPredicate(singleSourceCondition)
		// never swallow anything
		return combine(multipleSourcesCondition, rest), p
	}
}

// Return the unpushable condition and pushable condition
func extractCondition(condition ast.Expr) (unpushable ast.Expr, pushable ast.Expr) {
	s, hasDefault := getRefSources(condition)
	l := len(s)
	if hasDefault {
		l += 1
	}
	if l == 0 || (l == 1 && s[0] != ast.DefaultStream) {
		pushable = condition
		return
	}

	if be, ok := condition.(*ast.BinaryExpr); ok && be.OP == ast.AND {
		ul, pl := extractCondition(be.LHS)
		ur, pr := extractCondition(be.RHS)
		unpushable = combine(ul, ur)
		pushable = combine(pl, pr)
		return
	}

	// default case: all condition are unpushable
	return condition, nil
}

func (p *JoinPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.joins)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
