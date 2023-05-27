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
	return &p
}

func (p *OrderPlan) BuildExplainInfo(id int64) {
	info := ""
	p.baseLogicalPlan.ExplainInfo.Id = id
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p *OrderPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.SortFields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
