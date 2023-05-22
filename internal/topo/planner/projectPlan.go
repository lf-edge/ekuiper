// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

type ProjectPlan struct {
	baseLogicalPlan
	isAggregate      bool
	allWildcard      bool
	sendMeta         bool
	fields           ast.Fields
	colNames         [][]string
	aliasNames       []string
	exprNames        []string
	wildcardEmitters map[string]bool
	aliasFields      ast.Fields
	exprFields       ast.Fields
}

func (p ProjectPlan) Init() *ProjectPlan {
	p.allWildcard = false
	p.wildcardEmitters = make(map[string]bool)
	for _, field := range p.fields {
		if field.AName != "" {
			p.aliasFields = append(p.aliasFields, field)
			p.aliasNames = append(p.aliasNames, field.AName)
		} else {
			switch ft := field.Expr.(type) {
			case *ast.Wildcard:
				p.allWildcard = true
			case *ast.FieldRef:
				if ft.Name == "*" {
					p.wildcardEmitters[string(ft.StreamName)] = true
				} else {
					p.colNames = append(p.colNames, []string{ft.Name, string(ft.StreamName)})
				}
			default:
				p.exprNames = append(p.exprNames, field.Name)
				p.exprFields = append(p.exprFields, field)
			}
		}
	}
	p.baseLogicalPlan.self = &p
	return &p
}

func (p ProjectPlan) BuildExplainInfo(id int64) {
	info := ""
	p.baseLogicalPlan.ExplainInfo.Id = id
	p.baseLogicalPlan.ExplainInfo.Info = info
}

func (p *ProjectPlan) PruneColumns(fields []ast.Expr) error {
	f := getFields(p.fields)
	return p.baseLogicalPlan.PruneColumns(append(fields, f...))
}
