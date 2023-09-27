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
	"strconv"

	"github.com/lf-edge/ekuiper/pkg/ast"
)

type WatermarkPlan struct {
	baseLogicalPlan
	Emitters      []string
	SendWatermark bool
}

func (p WatermarkPlan) Init() *WatermarkPlan {
	p.baseLogicalPlan.self = &p
	p.setPlanType(WATERMARK)
	return &p
}

func (p *WatermarkPlan) BuildExplainInfo() {
	info := ""
	if len(p.Emitters) != 0 {
		info += "Emitters:[ "
		for i, emitter := range p.Emitters {
			info += emitter
			if i != len(p.Emitters)-1 {
				info += ", "
			}
		}
		info += " ], "
	}
	info += "SendWatermark:" + strconv.FormatBool(p.SendWatermark)
	p.baseLogicalPlan.ExplainInfo.Info = info
}

// PushDownPredicate watermark plan can not push down predicate. It must receive all tuples to process watermark
func (p *WatermarkPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	if condition != nil {
		f := FilterPlan{
			condition: condition,
		}.Init()
		f.SetChildren([]LogicalPlan{p})
		return nil, f
	}
	return nil, p.self
}
