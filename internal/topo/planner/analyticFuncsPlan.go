// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type AnalyticFuncsPlan struct {
	baseLogicalPlan
	funcs []*ast.Call
}

func (p AnalyticFuncsPlan) Init() *AnalyticFuncsPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *AnalyticFuncsPlan) BuildExplainInfo(id int64) {
	info := ""
	if p.funcs != nil && len(p.funcs) != 0 {
		info += "Funcs:[ "
		for i, v := range p.funcs {
			info += v.String()
			if i != len(p.funcs)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	p.baseLogicalPlan.ExplainInfo.Id = id
	p.baseLogicalPlan.ExplainInfo.Info = info
}

// PushDownPredicate this op must run before any filters
func (p *AnalyticFuncsPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	return condition, p
}

func (p *AnalyticFuncsPlan) PruneColumns(fields []ast.Expr) error {
	for _, f := range p.funcs {
		ff := getFields(f)
		fields = append(fields, ff...)
	}
	return p.baseLogicalPlan.PruneColumns(fields)
}
