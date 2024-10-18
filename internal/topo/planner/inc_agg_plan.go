// Copyright 2024 EMQ Technologies Co., Ltd.
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

type IncWindowPlan struct {
	baseLogicalPlan
	wType       ast.WindowType
	length      int
	dimensions  ast.Dimensions
	IncAggFuncs []*ast.Field
}

func (p *IncWindowPlan) BuildExplainInfo() {
	return
}

func (p *IncWindowPlan) PruneColumns(fields []ast.Expr) error {
	for _, IncAggFunc := range p.IncAggFuncs {
		fields = append(fields, getFields(IncAggFunc)...)
	}
	for _, dim := range p.dimensions {
		fields = append(fields, getFields(dim.Expr)...)
	}

	return p.baseLogicalPlan.PruneColumns(fields)
}

func (p IncWindowPlan) Init() *IncWindowPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(IncAggWindow)
	return &p
}
