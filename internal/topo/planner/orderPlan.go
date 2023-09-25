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

type OrderPlan struct {
	baseLogicalPlan
	SortFields ast.SortFields
}

func (p OrderPlan) Init() *OrderPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(ORDER)
	return &p
}

func (p *OrderPlan) BuildExplainInfo() {
	info := ""
	if p.SortFields != nil && len(p.SortFields) != 0 {
		info += "SortFields:[ "
		for i, field := range p.SortFields {
			info += field.String()
			if i != len(p.SortFields)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p *OrderPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.SortFields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
