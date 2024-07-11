// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type pushProjectionPlan struct{}

// pushProjectionPlan inject Projection Plan between the shared Datasource and its father only if the Plan have windowPlan
// We use Projection to remove the unused column before windowPlan in order to reduce memory consuming
func (pp *pushProjectionPlan) optimize(plan LogicalPlan) (LogicalPlan, error) {
	if pp.searchWindowPlan(plan) {
		ctx := &searchCtx{
			find: make([]*sharedSource, 0),
		}
		pp.searchSharedDataSource(ctx, plan, nil)
		if len(ctx.find) > 0 {
			pp.pushProjection(ctx)
		}
	}
	return plan, nil
}

func (pp *pushProjectionPlan) searchWindowPlan(plan LogicalPlan) bool {
	switch plan.(type) {
	case *WindowPlan:
		return true
	default:
	}
	for _, child := range plan.Children() {
		search := pp.searchWindowPlan(child)
		if search {
			return true
		}
	}
	return false
}

type searchCtx struct {
	find []*sharedSource
}

type sharedSource struct {
	ds     *DataSourcePlan
	father LogicalPlan
}

func (pp *pushProjectionPlan) searchSharedDataSource(ctx *searchCtx, plan, father LogicalPlan) {
	switch ds := plan.(type) {
	case *DataSourcePlan:
		if ds.streamStmt.Options.SHARED {
			ctx.find = append(ctx.find, &sharedSource{
				ds:     ds,
				father: father,
			})
		}
	default:
	}
	for _, child := range plan.Children() {
		pp.searchSharedDataSource(ctx, child, plan)
	}
}

func (pp *pushProjectionPlan) pushProjection(ctx *searchCtx) {
	for _, search := range ctx.find {
		p := ProjectPlan{
			fields:      buildFields(search.ds),
			isAggregate: false,
			sendMeta:    false,
			enableLimit: false,
		}.Init()
		p.children = []LogicalPlan{search.ds}
		for index, child := range search.father.Children() {
			if child.ID() == search.ds.ID() {
				search.father.Children()[index] = p
				break
			}
		}
	}
}

func buildFields(ds *DataSourcePlan) []ast.Field {
	want := make([]ast.Field, 0)
	if ds.isWildCard {
		want = append(want, ast.Field{Expr: &ast.Wildcard{}})
		return want
	}
	for k := range ds.streamFields {
		want = append(want, ast.Field{Name: k, Expr: &ast.FieldRef{Name: k, StreamName: ds.streamStmt.Name}})
	}
	return want
}

func (pp *pushProjectionPlan) name() string {
	return "push_projection"
}
