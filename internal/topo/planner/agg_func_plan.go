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
