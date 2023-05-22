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

type FilterPlan struct {
	baseLogicalPlan
	condition ast.Expr
}

func (p FilterPlan) Init() *FilterPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p FilterPlan) BuildExplainInfo(id int64) {
	info := ""
	p.baseLogicalPlan.ExplainInfo.Id = id
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p *FilterPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	// if no child, swallow all conditions
	a := combine(condition, p.condition)
	if len(p.children) == 0 {
		p.condition = a
		return nil, p
	}

	rest, _ := p.baseLogicalPlan.PushDownPredicate(a)

	if rest != nil {
		p.condition = rest
		return nil, p
	} else if len(p.children) == 1 {
		// eliminate this filter
		return nil, p.children[0]
	} else {
		return nil, p
	}
}

func (p *FilterPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.condition)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
