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

package ast

import (
	"math"
	"regexp"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func init() {
	testx.InitEnv("ast")
}

func Test_exprStringPlan(t *testing.T) {
	re1, _ := regexp.Compile("^foo$")
	test := []struct {
		e   Expr
		res string
	}{
		{
			e: &BetweenExpr{
				Lower: &IntegerLiteral{
					Val: 0,
				},
				Higher: &IntegerLiteral{
					Val: 10,
				},
			},
			res: "betweenExpr:{ 0, 10 }",
		},
		{
			e: &BinaryExpr{
				OP: SUBSET,
				LHS: &FieldRef{
					StreamName: "src1",
					Name:       "myarray",
				},
				RHS: &IndexExpr{Index: &FieldRef{
					StreamName: "src1",
					Name:       "temp",
				}},
			},
			res: "binaryExpr:{ src1.myarray[src1.temp] }",
		},
		{
			e:   &BooleanLiteral{Val: true},
			res: "true",
		},
		{
			e: &Call{Name: "count", FuncId: 0, Args: []Expr{&Wildcard{
				Token: ASTERISK,
			}}, FuncType: FuncTypeAgg},
			res: "Call:{ name:count, args:[*] }",
		},
		{
			e: &CaseExpr{
				WhenClauses: []*WhenClause{
					{
						Expr: &BinaryExpr{
							OP: BETWEEN,
							LHS: &Call{
								Name:     "lag",
								FuncId:   0,
								FuncType: FuncType(0),
								Args: []Expr{
									&FieldRef{
										StreamName: "src1",
										Name:       "temp",
									},
								},
								CachedField: "$$a_lag_0",
								Cached:      true,
								WhenExpr: &BinaryExpr{
									OP: GT,
									LHS: &Call{
										Name:     "lag",
										FuncId:   1,
										FuncType: FuncType(0),
										Args: []Expr{
											&FieldRef{
												StreamName: "src1",
												Name:       "id1",
											},
										},
										CachedField: "$$a_lag_1",
										Cached:      true,
									},
									RHS: &IntegerLiteral{
										Val: 1,
									},
								},
							},
							RHS: &BetweenExpr{
								Lower: &IntegerLiteral{
									Val: 0,
								},
								Higher: &IntegerLiteral{
									Val: 10,
								},
							},
						},
						Result: &IntegerLiteral{
							Val: 1,
						},
					},
					{
						&BinaryExpr{
							OP: BETWEEN,
							LHS: &Call{
								Name:     "lag",
								FuncId:   0,
								FuncType: FuncType(0),
								Args: []Expr{
									&FieldRef{
										StreamName: "src1",
										Name:       "temp",
									},
								},
								CachedField: "$$a_lag_0",
								Cached:      true,
								WhenExpr: &BinaryExpr{
									OP: GT,
									LHS: &Call{
										Name:     "lag",
										FuncId:   1,
										FuncType: FuncType(0),
										Args: []Expr{
											&FieldRef{
												StreamName: "src1",
												Name:       "id1",
											},
										},
										CachedField: "$$a_lag_1",
										Cached:      true,
									},
									RHS: &IntegerLiteral{
										Val: 1,
									},
								},
							},
							RHS: &BetweenExpr{
								Lower: &IntegerLiteral{
									Val: 0,
								},
								Higher: &IntegerLiteral{
									Val: 10,
								},
							},
						},
						&IntegerLiteral{
							Val: 2,
						},
					},
				},
				ElseClause: &IntegerLiteral{
					Val: 0,
				},
				Value: &IntegerLiteral{
					Val: 12,
				},
			},
			res: "caseExprValue:{ value:{ 12 }, whenClauses:[{ whenClause:{ binaryExpr:{ Call:{ name:lag, args:[src1.temp], when:{ binaryExpr:{ Call:{ name:lag, args:[src1.id1] } > 1 } } } BETWEEN betweenExpr:{ 0, 10 } } } }, { whenClause:{ binaryExpr:{ Call:{ name:lag, args:[src1.temp], when:{ binaryExpr:{ Call:{ name:lag, args:[src1.id1] } > 1 } } } BETWEEN betweenExpr:{ 0, 10 } } } }] }",
		},
		{
			e:   &JsonFieldRef{Name: "Device"},
			res: "jsonFieldName:Device",
		},
		{
			e:   &NumberLiteral{Val: 1.23},
			res: "1.230000",
		},
		{
			e:   &StringLiteral{Val: "v1"},
			res: "v1",
		},
		{
			e:   &TimeLiteral{Val: 2},
			res: "WS",
		},
		{
			e: &MetaRef{
				Name:       "device",
				StreamName: DefaultStream,
			},
			res: "metaRef:{ streamName:$$default, fieldName:device }",
		},
		{
			e:   &PartitionExpr{Exprs: []Expr{&FieldRef{Name: "temp", StreamName: "src1"}, &FieldRef{Name: "current", StreamName: "src2"}}},
			res: "PartitionExpr:[ src1.temp, src2.current ]",
		},
		{
			e:   &SortField{Uname: "name", Name: "name", Ascending: true, FieldExpr: &FieldRef{Name: "name", StreamName: DefaultStream}},
			res: "sortField:{ name:name, ascending:true, fieldExpr:{ $$default.name } }",
		},
		{
			e:   &BracketExpr{Expr: &ColonExpr{Start: &IntegerLiteral{Val: 0}, End: &IntegerLiteral{Val: math.MinInt32}}},
			res: "bracketExpr:{ ColonExpr:{ start:{ 0 }, end:{ -2147483648 } } }",
		},
		{
			e:   &ArrowExpr{Expr: &ColonExpr{Start: &IntegerLiteral{Val: 0}, End: &IntegerLiteral{Val: math.MinInt32}}},
			res: "arrowExpr:{ ColonExpr:{ start:{ 0 }, end:{ -2147483648 } } }",
		},
		{
			e: &ValueSetExpr{
				LiteralExprs: []Expr{&StringLiteral{"A"}, &StringLiteral{"B"}},
				ArrayExpr:    &StringLiteral{"A, B"},
			},
			res: "valueSetExpr:{ literalExprs:[A, B], arrayExpr:{ A, B } }",
		},
		{
			e: &ColFuncField{
				Name: "ABC",
				Expr: &StringLiteral{Val: ""},
			},
			res: "colFuncField:{ name: ABC, expr:{  } }",
		},
		{
			e:   &LikePattern{Expr: &StringLiteral{Val: "foo"}, Pattern: re1},
			res: "likePattern:^foo$",
		},
		{
			e: &LimitExpr{
				LimitCount: &IntegerLiteral{Val: 10},
			},
			res: "limitExpr:{ 10 }",
		},
	}

	for i := 0; i < len(test); i++ {
		res := test[i].res
		str := test[i].e.String()
		if str != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, str)
		}
	}
}
