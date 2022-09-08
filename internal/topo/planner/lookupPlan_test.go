// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
)

func TestValidate(t *testing.T) {
	var tests = []struct {
		p  *LookupPlan
		v  bool
		c  ast.Expr
		k  []string
		vv []ast.Expr
	}{
		{ // 0
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.EQ,
						LHS: &ast.FieldRef{
							StreamName: "left",
							Name:       "device_id",
						},
						RHS: &ast.FieldRef{
							StreamName: "good",
							Name:       "id",
						},
					},
				},
			},
			v: true,
			k: []string{
				"id",
			},
			vv: []ast.Expr{
				&ast.FieldRef{
					StreamName: "left",
					Name:       "device_id",
				},
			},
			c: nil,
		}, { // 1
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.GT,
						LHS: &ast.FieldRef{
							StreamName: "left",
							Name:       "device_id",
						},
						RHS: &ast.FieldRef{
							StreamName: "good",
							Name:       "id",
						},
					},
				},
			},
			v: false,
			c: nil,
		}, { // 2
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.EQ,
						LHS: &ast.FieldRef{
							StreamName: "left",
							Name:       "device_id",
						},
						RHS: &ast.IntegerLiteral{Val: 23},
					},
				},
			},
			v: false,
			c: nil,
		}, { // 3
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.OR,
						LHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id",
							},
						},
						RHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id1",
							},
						},
					},
				},
			},
			v: false,
			c: nil,
		}, { // 4
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.AND,
						LHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id",
							},
						},
						RHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id1",
							},
						},
					},
				},
			},
			v: true,
			k: []string{
				"id", "id1",
			},
			vv: []ast.Expr{
				&ast.FieldRef{
					StreamName: "left",
					Name:       "device_id",
				},
				&ast.FieldRef{
					StreamName: "left",
					Name:       "device_id",
				},
			},
			c: nil,
		}, { // 5
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.AND,
						LHS: &ast.BinaryExpr{
							OP: ast.GT,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.IntegerLiteral{
								Val: 33,
							},
						},
						RHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id1",
							},
						},
					},
				},
			},
			v: true,
			k: []string{
				"id1",
			},
			vv: []ast.Expr{
				&ast.FieldRef{
					StreamName: "left",
					Name:       "device_id",
				},
			},
			c: &ast.BinaryExpr{
				OP: ast.GT,
				LHS: &ast.FieldRef{
					StreamName: "left",
					Name:       "device_id",
				},
				RHS: &ast.IntegerLiteral{
					Val: 33,
				},
			},
		}, { // 6
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.AND,
						LHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id",
							},
						},
						RHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "good",
								Name:       "id",
							},
						},
					},
				},
			},
			v: false,
		}, { // 7
			p: &LookupPlan{
				joinExpr: ast.Join{
					Name:     "good",
					JoinType: 0,
					Expr: &ast.BinaryExpr{
						OP: ast.AND,
						LHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "right",
								Name:       "id",
							},
						},
						RHS: &ast.BinaryExpr{
							OP: ast.EQ,
							LHS: &ast.FieldRef{
								StreamName: "left",
								Name:       "device_id",
							},
							RHS: &ast.FieldRef{
								StreamName: "right",
								Name:       "id2",
							},
						},
					},
				},
			},
			v: false,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		rv := tt.p.validateAndExtractCondition()
		if rv != tt.v {
			t.Errorf("case %d: expect validate %v but got %v", i, tt.v, rv)
			continue
		}
		if rv {
			if !reflect.DeepEqual(tt.c, tt.p.conditions) {
				t.Errorf("case %d: expect conditions %v but got %v", i, tt.c, tt.p.conditions)
				continue
			}
			if !reflect.DeepEqual(tt.k, tt.p.keys) {
				t.Errorf("case %d: expect keys %v but got %v", i, tt.k, tt.p.keys)
				continue
			}
			if !reflect.DeepEqual(tt.vv, tt.p.valvars) {
				t.Errorf("case %d: expect val vars %v but got %v", i, tt.vv, tt.p.valvars)
			}
		}
	}
}
