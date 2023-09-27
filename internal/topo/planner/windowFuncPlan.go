// Copyright 2023 EMQ Technologies Co., Ltd.
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

type WindowFuncPlan struct {
	baseLogicalPlan
	windowFuncFields ast.Fields
}

func (p WindowFuncPlan) Init() *WindowFuncPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(WINDOWFUNC)
	return &p
}

func (p *WindowFuncPlan) BuildExplainInfo() {
	info := ""
	if p.windowFuncFields != nil && len(p.windowFuncFields) != 0 {
		info += "windowFuncFields:[ "
		for i, field := range p.windowFuncFields {
			info += "{name:" + field.GetName()
			if field.Expr != nil {
				info += ", expr:" + field.Expr.String()
			}
			info += "}"
			if i != len(p.windowFuncFields)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	p.baseLogicalPlan.ExplainInfo.Info = info
}
