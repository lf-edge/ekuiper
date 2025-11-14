// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

const (
	DefaultRetainSize = 1
	MaxRetainSize     = 9999
)

type JoinAlignPlan struct {
	baseLogicalPlan
	Emitters []string
	// retain size for each table emitter
	Sizes []int
}

func (p *JoinAlignPlan) BuildExplainInfo() {
	info := ""
	if len(p.Emitters) != 0 {
		info += "Emitters:[ "
		for i, emitter := range p.Emitters {
			info += emitter
			if i != len(p.Emitters)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p JoinAlignPlan) Init() *JoinAlignPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(JOINALIGN)
	return &p
}

// PushDownPredicate Push down to table first, then push to window
func (p *JoinAlignPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	if len(p.children) == 0 {
		return condition, p.self
	}
	rest := condition
	for i, child := range p.children {
		if _, ok := child.(*DataSourcePlan); ok {
			var newChild LogicalPlan
			rest, newChild = child.PushDownPredicate(rest)
			p.children[i] = newChild
		}
	}
	return rest, p.self
}
