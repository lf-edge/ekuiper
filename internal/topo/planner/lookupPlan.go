// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"sort"

	"github.com/modern-go/reflect2"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

// LookupPlan is the plan for table lookup and then merged/joined
type LookupPlan struct {
	baseLogicalPlan
	joinExpr   ast.Join
	keys       []string
	fields     []string
	valvars    []ast.Expr
	options    *ast.Options
	conditions ast.Expr
}

// Init must run validateAndExtractCondition before this func
func (p LookupPlan) Init() *LookupPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(LOOKUP)
	return &p
}

func (p *LookupPlan) BuildExplainInfo() {
	info := ""
	if p.conditions != nil {
		info += "Condition:{ "
		info += p.conditions.String()
		info += " }"
	}
	if !reflect2.IsNil(p.joinExpr) {
		join := p.joinExpr
		if p.conditions != nil {
			info += ", "
		}
		info += "Join:{ joinType:" + join.JoinType.String()
		if join.Expr != nil {
			info += ", expr:" + join.Expr.String()
		}
		info += " }"
	}
	p.baseLogicalPlan.ExplainInfo.Info = info
}

// PushDownPredicate do not deal with conditions, push down or return up
func (p *LookupPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	a := combine(condition, p.conditions)
	if len(p.children) == 0 {
		return a, p.self
	}
	unpushable, pushable := extractLookupCondition(a, p.joinExpr.Name)
	rest, _ := p.baseLogicalPlan.PushDownPredicate(pushable)
	restAll := combine(unpushable, rest)
	// Swallow all filter conditions. If there are other filter plans, there may have multiple filters
	if restAll != nil {
		// Add a filter plan for children
		f := FilterPlan{
			condition: restAll,
		}.Init()
		f.SetChildren([]LogicalPlan{p})
		return nil, f
	}
	return nil, p.self
}

// Return the unpushable condition and pushable condition
func extractLookupCondition(condition ast.Expr, tableName string) (unpushable ast.Expr, pushable ast.Expr) {
	s, hasDefault := getRefSources(condition)
	l := len(s)
	if hasDefault {
		l += 1
	}
	if l == 0 || (l == 1 && s[0] != ast.DefaultStream && s[0] != ast.StreamName(tableName)) {
		pushable = condition
		return
	}

	if be, ok := condition.(*ast.BinaryExpr); ok && be.OP == ast.AND {
		ul, pl := extractLookupCondition(be.LHS, tableName)
		ur, pr := extractLookupCondition(be.RHS, tableName)
		unpushable = combine(ul, ur)
		pushable = combine(pl, pr)
		return
	}

	// default case: all condition are unpushable
	return condition, nil
}

// validateAndExtractCondition Make sure the join condition is equi-join and extreact other conditions
func (p *LookupPlan) validateAndExtractCondition() bool {
	equi, conditions := flatConditions(p.joinExpr.Expr)
	// No equal predict condition found
	if len(equi) == 0 {
		return false
	}
	if len(conditions) > 0 {
		p.conditions = conditions[0]
		for _, c := range conditions[1:] {
			p.conditions = &ast.BinaryExpr{OP: ast.AND, LHS: p.conditions, RHS: c}
		}
	}

	strName := p.joinExpr.Name
	kset := make(map[string]struct{})
	// Extract equi-join condition
	for _, c := range equi {
		lref, lok := c.LHS.(*ast.FieldRef)
		rref, rok := c.RHS.(*ast.FieldRef)
		if lok && rok {
			if lref.StreamName == rref.StreamName {
				continue
			}
			if string(lref.StreamName) == strName {
				if _, ok := kset[lref.Name]; ok {
					return false
				}
				kset[lref.Name] = struct{}{}
				p.keys = append(p.keys, lref.Name)
				p.valvars = append(p.valvars, rref)
			} else if string(rref.StreamName) == strName {
				if _, ok := kset[rref.Name]; ok {
					return false
				}
				kset[rref.Name] = struct{}{}
				p.keys = append(p.keys, rref.Name)
				p.valvars = append(p.valvars, lref)
			} else {
				continue
			}
		} else if lok {
			if string(lref.StreamName) == strName {
				if _, ok := kset[lref.Name]; ok {
					return false
				}
				kset[lref.Name] = struct{}{}
				p.keys = append(p.keys, lref.Name)
				p.valvars = append(p.valvars, c.RHS)
			} else {
				continue
			}
		} else if rok {
			if string(rref.StreamName) == strName {
				if _, ok := kset[rref.Name]; ok {
					return false
				}
				kset[rref.Name] = struct{}{}
				p.keys = append(p.keys, rref.Name)
				p.valvars = append(p.valvars, c.LHS)
			} else {
				continue
			}
		} else {
			continue
		}
	}
	return len(kset) > 0
}

// flatConditions flat the join condition. Only binary condition of EQ and AND are allowed
func flatConditions(condition ast.Expr) ([]*ast.BinaryExpr, []ast.Expr) {
	if be, ok := condition.(*ast.BinaryExpr); ok {
		switch be.OP {
		case ast.EQ:
			return []*ast.BinaryExpr{be}, []ast.Expr{}
		case ast.AND:
			e1, e2 := flatConditions(be.LHS)
			e3, e4 := flatConditions(be.RHS)
			return append(e1, e3...), append(e2, e4...)
		default:
			return []*ast.BinaryExpr{}, []ast.Expr{condition}
		}
	}
	return []*ast.BinaryExpr{}, []ast.Expr{condition}
}

func (p *LookupPlan) PruneColumns(fields []ast.Expr) error {
	newFields := make([]ast.Expr, 0, len(fields))
	isWildcard := false
	lookupTableName := p.joinExpr.Name
	fieldMap := make(map[string]struct{})
	for _, field := range fields {
		switch f := field.(type) {
		case *ast.Wildcard:
			isWildcard = true
		case *ast.FieldRef:
			if !isWildcard {
				if f.StreamName == ast.DefaultStream {
					if f.Name == "*" {
						isWildcard = true
						continue
					} else {
						fieldMap[f.Name] = struct{}{}
					}
				} else if string(f.StreamName) == lookupTableName {
					if f.Name == "*" {
						isWildcard = true
					} else {
						fieldMap[f.Name] = struct{}{}
					}
					continue
				}
			}
		case *ast.SortField:
			if !isWildcard {
				fieldMap[f.Name] = struct{}{}
				continue
			}
		}
		newFields = append(newFields, field)
	}
	if !isWildcard {
		p.fields = make([]string, 0, len(fieldMap))
		for k := range fieldMap {
			p.fields = append(p.fields, k)
		}
		sort.Strings(p.fields)
	}
	for _, f := range getFields(p.joinExpr.Expr) {
		fr, ok := f.(*ast.FieldRef)
		if ok {
			if fr.IsColumn() && string(fr.StreamName) != lookupTableName {
				newFields = append(newFields, fr)
			}
		}
	}
	return p.baseLogicalPlan.PruneColumns(newFields)
}
