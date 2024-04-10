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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestFilterPlan_ExtractStateFunc(t *testing.T) {
	tests := []struct {
		name         string
		condition    ast.Expr
		newCondition ast.Expr
		stateFuncs   []*ast.Call
	}{
		{
			name: "test extract one",
			condition: &ast.BinaryExpr{
				OP: ast.AND,
				LHS: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.FieldRef{
						Name: "a",
					},
					RHS: &ast.IntegerLiteral{
						Val: 1,
					},
				},
				RHS: &ast.Call{
					Name: "last_hit_count",
				},
			},
			newCondition: &ast.BinaryExpr{
				OP: ast.AND,
				LHS: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.FieldRef{
						Name: "a",
					},
					RHS: &ast.IntegerLiteral{
						Val: 1,
					},
				},
				RHS: &ast.Call{
					Name:   "last_hit_count",
					Cached: true,
				},
			},
			stateFuncs: []*ast.Call{
				{
					Name: "last_hit_count",
				},
			},
		},
		{
			name: "test extract multiple",
			condition: &ast.BinaryExpr{
				OP: ast.AND,
				LHS: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.Call{
						Name: "last_hit_time",
					},
					RHS: &ast.IntegerLiteral{
						Val: 1,
					},
				},
				RHS: &ast.Call{
					Name: "last_hit_count",
				},
			},
			newCondition: &ast.BinaryExpr{
				OP: ast.AND,
				LHS: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.Call{
						Name:   "last_hit_time",
						Cached: true,
					},
					RHS: &ast.IntegerLiteral{
						Val: 1,
					},
				},
				RHS: &ast.Call{
					Name:   "last_hit_count",
					Cached: true,
				},
			},
			stateFuncs: []*ast.Call{
				{
					Name: "last_hit_time",
				}, {
					Name: "last_hit_count",
				},
			},
		},
		{
			name: "test extract none",
			condition: &ast.BinaryExpr{
				OP: ast.GT,
				LHS: &ast.Call{
					Name: "event_time",
				},
				RHS: &ast.IntegerLiteral{
					Val: 1,
				},
			},
			newCondition: &ast.BinaryExpr{
				OP: ast.GT,
				LHS: &ast.Call{
					Name: "event_time",
				},
				RHS: &ast.IntegerLiteral{
					Val: 1,
				},
			},
			stateFuncs: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &FilterPlan{
				condition: tt.condition,
			}
			p.ExtractStateFunc()
			assert.Equal(t, tt.newCondition, p.condition)
			assert.Equal(t, tt.stateFuncs, p.stateFuncs)
		})
	}
}
