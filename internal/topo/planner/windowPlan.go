// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type WindowPlan struct {
	baseLogicalPlan
	triggerCondition ast.Expr
	condition        ast.Expr
	wtype            ast.WindowType
	delay            int64
	length           int
	interval         int // If interval is not set, it is equals to Length
	timeUnit         ast.Token
	limit            int // If limit is not positive, there will be no limit
	isEventTime      bool

	stateFuncs []*ast.Call
}

func (p WindowPlan) Init() *WindowPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(WINDOW)
	return &p
}

func (p *WindowPlan) BuildExplainInfo() {
	t := p.wtype.String()
	info := "{ length:" + strconv.Itoa(p.length) + ", "
	info += "windowType:" + t
	if p.condition != nil {
		info += ", condition:" + p.condition.String()
	}
	if len(p.stateFuncs) != 0 {
		info += ", stateFuncs:[ "
		for _, stateFunc := range p.stateFuncs {
			info += stateFunc.String()
		}
		info += " ]"
	}
	info += ", limit: " + strconv.Itoa(p.limit) + " }"
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p *WindowPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	// not time window depends on the event, so should not filter any
	if p.wtype == ast.COUNT_WINDOW || p.wtype == ast.SLIDING_WINDOW {
		return condition, p
	} else if p.isEventTime {
		// TODO event time filter, need event window op support
		//p.condition = combine(condition, p.condition)
		//// push nil condition won't return any
		//p.baseLogicalPlan.PushDownPredicate(nil)
		// return nil, p
		return condition, p
	} else {
		// Presume window condition are only one table related.
		// TODO window condition validation
		a := combine(condition, p.condition)
		p.condition, _ = p.baseLogicalPlan.PushDownPredicate(a)
		return nil, p
	}
}

func (p *WindowPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.condition)
	f = append(f, getFields(p.triggerCondition)...)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}

func (p *WindowPlan) ExtractStateFunc() {
	aliases := make(map[string]ast.Expr)
	ast.WalkFunc(p.triggerCondition, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			p.transform(f)
		case *ast.FieldRef:
			if f.AliasRef != nil {
				aliases[f.Name] = f.AliasRef.Expression
			}
		}
		return true
	})
	for _, ex := range aliases {
		ast.WalkFunc(ex, func(n ast.Node) bool {
			switch f := n.(type) {
			case *ast.Call:
				p.transform(f)
			}
			return true
		})
	}
}

func (p *WindowPlan) transform(f *ast.Call) {
	if _, ok := xsql.ImplicitStateFuncs[f.Name]; ok {
		f.Cached = true
		p.stateFuncs = append(p.stateFuncs, &ast.Call{
			Name:     f.Name,
			FuncId:   f.FuncId,
			FuncType: f.FuncType,
		})
	}
}
