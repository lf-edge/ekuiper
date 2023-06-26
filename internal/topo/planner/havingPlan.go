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
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type HavingPlan struct {
	baseLogicalPlan
	condition  ast.Expr
	stateFuncs []*ast.Call
}

func (p HavingPlan) Init() *HavingPlan {
	p.baseLogicalPlan.self = &p
	return &p
}

func (p *HavingPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.condition)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}

func (p *HavingPlan) ExtractStateFunc() {
	ast.WalkFunc(p.condition, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if _, ok := xsql.ImplicitStateFuncs[f.Name]; ok {
				f.Cached = true
				p.stateFuncs = append(p.stateFuncs, &ast.Call{
					Name:     f.Name,
					FuncId:   f.FuncId,
					FuncType: f.FuncType,
				})
			}
		}
		return true
	})
}
