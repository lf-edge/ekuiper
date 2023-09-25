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
	"bytes"
	"encoding/json"

	"github.com/lf-edge/ekuiper/pkg/ast"
)

type LogicalPlan interface {
	ExplainInfo
	Children() []LogicalPlan
	SetChildren(children []LogicalPlan)
	// PushDownPredicate pushes down the filter in the filter/where/on/having clauses as deeply as possible.
	// It will accept a condition that is an expression slice, and return the expressions that can't be pushed.
	// It is also return the new tree of plan as it can possibly change the tree
	PushDownPredicate(ast.Expr) (ast.Expr, LogicalPlan)
	// PruneColumns Prune the unused columns in the data source level, by pushing all needed columns down
	PruneColumns(fields []ast.Expr) error
}

type baseLogicalPlan struct {
	children []LogicalPlan
	// Can be used to return the derived instance from the base type
	self LogicalPlan
	// Interface for explaining
	ExplainInfo PlanExplainInfo
}

type ExplainInfo interface {
	ID() int64
	Type() string
	ChildrenID() []int64
	Explain() string
	BuildExplainInfo()
	SetID(id int64)
}

type PlanExplainInfo struct {
	T        PlanType `json:"type"`
	Info     string   `json:"info"`
	ID       int64    `json:"id"`
	Children []int64  `json:"children"`
}

func (p *baseLogicalPlan) Explain() string {
	p.ExplainInfo.Children = p.ChildrenID()
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	jsonEncoder.Encode(p.ExplainInfo)
	return bf.String()
}

func (p *baseLogicalPlan) BuildExplainInfo() {
	p.self.BuildExplainInfo()
}

func (p *baseLogicalPlan) SetID(id int64) {
	p.ExplainInfo.ID = id
}

func (p *baseLogicalPlan) Type() string {
	return string(p.ExplainInfo.T)
}

func (p *baseLogicalPlan) ID() int64 {
	return p.ExplainInfo.ID
}

func (p *baseLogicalPlan) ChildrenID() []int64 {
	var children []int64
	for _, child := range p.Children() {
		children = append(children, child.ID())
	}
	return children
}

func (p *baseLogicalPlan) setPlanType(planType PlanType) {
	p.ExplainInfo.T = planType
}

func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

func (p *baseLogicalPlan) SetChildren(children []LogicalPlan) {
	p.children = children
}

// PushDownPredicate By default, push down the predicate to the first child instead of the children
// as most plan cannot have multiple children
func (p *baseLogicalPlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	if len(p.children) == 0 {
		return condition, p.self
	}
	rest := condition
	for i, child := range p.children {
		var newChild LogicalPlan
		rest, newChild = child.PushDownPredicate(rest)
		p.children[i] = newChild
	}
	return rest, p.self
}

func (p *baseLogicalPlan) PruneColumns(fields []ast.Expr) error {
	for _, child := range p.children {
		err := child.PruneColumns(fields)
		if err != nil {
			return err
		}
	}
	return nil
}

type PlanType string

const (
	AGGREGATE     PlanType = "AggregatePlan"
	ANALYTICFUNCS PlanType = "AnalyticFuncsPlan"
	DATASOURCE    PlanType = "DataSourcePlan"
	FILTER        PlanType = "FilterPlan"
	HAVING        PlanType = "HavingPlan"
	JOINALIGN     PlanType = "JoinAlignPlan"
	JOIN          PlanType = "JoinPlan"
	LOOKUP        PlanType = "LookupPlan"
	ORDER         PlanType = "OrderPlan"
	PROJECT       PlanType = "ProjectPlan"
	PROJECTSET    PlanType = "ProjectSetPlan"
	WINDOW        PlanType = "WindowPlan"
	WINDOWFUNC    PlanType = "WindowFuncPlan"
	WATERMARK     PlanType = "WatermarkPlan"
)
