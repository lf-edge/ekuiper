// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestDataSourcePlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *DataSourcePlan
		res string
		t   string
	}{
		{
			p: &DataSourcePlan{
				name: "test1",
				fields: map[string]*ast.JsonStreamField{
					"field1": {},
					"field2": {},
					"field3": {},
				},
			},
			res: `{"op":"DataSourcePlan_0","info":"StreamName: test1, Fields:[ field1, field2, field3 ]"}`,
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test2",
				streamFields: map[string]*ast.JsonStreamField{
					"a": {},
					"b": {},
					"c": {},
				},
			},
			res: `{"op":"DataSourcePlan_1","info":"StreamName: test2, StreamFields:[ a, b, c ]"}`,
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test3",
				fields: map[string]*ast.JsonStreamField{
					"id":      {},
					"column1": {},
					"column2": {},
				},
				streamFields: map[string]*ast.JsonStreamField{
					"s1": {},
					"s2": {},
					"s3": {},
				},
			},
			res: `{"op":"DataSourcePlan_2","info":"StreamName: test3, Fields:[ column1, column2, id ], StreamFields:[ s1, s2, s3 ]"}`,
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test4",
			},
			res: `{"op":"DataSourcePlan_3","info":"StreamName: test4"}`,
			t:   "DataSourcePlan",
		},
		{
			p:   &DataSourcePlan{},
			res: `{"op":"DataSourcePlan_4","info":""}`,
			t:   "DataSourcePlan",
		},
	}
	test[1].p.SetChildren([]LogicalPlan{test[2].p})
	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
	}
}

func TestAggregatePlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *AggregatePlan
		res string
		t   string
	}{
		{
			p: &AggregatePlan{
				dimensions: ast.Dimensions{
					ast.Dimension{
						Expr: &ast.Call{
							Name: "lpad", Args: []ast.Expr{
								&ast.FieldRef{Name: "name", StreamName: ast.DefaultStream},
								&ast.IntegerLiteral{Val: 1},
							},
						},
					},
				},
			},
			res: `{"op":"AggregatePlan_0","info":"Dimension:{ Call:{ name:lpad, args:[$$default.name, 1] } }"}`,
			t:   "AggregatePlan",
		},
		{
			p: &AggregatePlan{
				dimensions: ast.Dimensions{
					ast.Dimension{
						Expr: &ast.Window{
							WindowType: ast.SLIDING_WINDOW,
							Length:     &ast.IntegerLiteral{Val: 5},
							Interval:   &ast.IntegerLiteral{Val: 0},
							TimeUnit:   &ast.TimeLiteral{Val: ast.MS},
							TriggerCondition: &ast.BinaryExpr{
								OP:  ast.GT,
								LHS: &ast.FieldRef{Name: "a", StreamName: ast.DefaultStream},
								RHS: &ast.IntegerLiteral{Val: 5},
							},
							Delay: &ast.IntegerLiteral{Val: 0},
						},
					},
				},
			},
			res: `{"op":"AggregatePlan_1","info":"Dimension:{ window:{ windowType:SLIDING_WINDOW, timeUnit: MS  } }"}`,
			t:   "AggregatePlan",
		},
		{
			p: &AggregatePlan{
				dimensions: ast.Dimensions{
					ast.Dimension{Expr: &ast.FieldRef{Name: "department", StreamName: ast.DefaultStream}},
				},
			},
			res: `{"op":"AggregatePlan_2","info":"Dimension:{ $$default.department }"}`,
			t:   "AggregatePlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[2].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestAnalyticFuncsPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *AnalyticFuncsPlan
		res string
		t   string
	}{
		{
			p: &AnalyticFuncsPlan{
				funcs: []*ast.Call{
					{
						Name:        "lag",
						FuncId:      2,
						CachedField: "$$a_lag_2",
						Args: []ast.Expr{&ast.FieldRef{
							Name:       "temp",
							StreamName: "src1",
						}},
					},
				},
				fieldFuncs: []*ast.Call{
					{
						Name: "lag", FuncId: 0, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}},
					},
					{
						Name: "latest", FuncId: 1, CachedField: "$$a_latest_1", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}}, Partition: &ast.PartitionExpr{Exprs: []ast.Expr{&ast.FieldRef{Name: "temp", StreamName: "src1"}}},
					},
				},
			},
			res: `{"op":"AnalyticFuncsPlan_0","info":"Funcs:[ Call:{ name:lag, args:[src1.temp] } ], FieldFuncs:[ Call:{ name:lag, args:[src1.name] }, Call:{ name:latest, args:[Call:{ name:lag, args:[src1.name] }] } ]"}`,
			t:   "AnalyticFuncsPlan",
		},
		{
			p: &AnalyticFuncsPlan{
				fieldFuncs: []*ast.Call{
					{
						Name:     "lag",
						FuncId:   1,
						FuncType: ast.FuncType(0),
						Args: []ast.Expr{
							&ast.FieldRef{
								StreamName: "src1",
								Name:       "id1",
							},
						},
						CachedField: "$$a_lag_1",
					},
					{
						Name:     "lag",
						FuncId:   0,
						FuncType: ast.FuncType(0),
						Args: []ast.Expr{
							&ast.FieldRef{
								StreamName: "src1",
								Name:       "temp",
							},
						},
						CachedField: "$$a_lag_0",
						WhenExpr: &ast.BinaryExpr{
							OP: ast.GT,
							LHS: &ast.Call{
								Name:     "lag",
								FuncId:   1,
								FuncType: ast.FuncType(0),
								Args: []ast.Expr{
									&ast.FieldRef{
										StreamName: "src1",
										Name:       "id1",
									},
								},
								CachedField: "$$a_lag_1",
								Cached:      true,
							},
							RHS: &ast.IntegerLiteral{
								Val: 1,
							},
						},
					},
				},
			},
			res: `{"op":"AnalyticFuncsPlan_1","info":"FieldFuncs:[ Call:{ name:lag, args:[src1.id1] }, Call:{ name:lag, args:[src1.temp], when:{ binaryExpr:{ Call:{ name:lag, args:[src1.id1] } > 1 } } } ]"}`,
			t:   "AnalyticFuncsPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestFilterPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *FilterPlan
		res string
		t   string
	}{
		{
			p: &FilterPlan{
				condition: &ast.BinaryExpr{
					LHS: &ast.FieldRef{Name: "name", StreamName: "src1"},
					OP:  ast.EQ,
					RHS: &ast.StringLiteral{Val: "v1"},
				},
			},
			res: `{"op":"FilterPlan_0","info":"Condition:{ binaryExpr:{ src1.name = v1 } }, "}`,
			t:   "FilterPlan",
		},
		{
			p: &FilterPlan{
				condition: &ast.BinaryExpr{
					RHS: &ast.BinaryExpr{
						OP:  ast.GT,
						LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
						RHS: &ast.IntegerLiteral{Val: 20},
					},
					OP: ast.AND,
					LHS: &ast.BinaryExpr{
						OP:  ast.GT,
						LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
						RHS: &ast.IntegerLiteral{Val: 111},
					},
				},
			},
			res: "{\"op\":\"FilterPlan_1\",\"info\":\"Condition:{ binaryExpr:{ binaryExpr:{ src1.id1 > 111 } AND binaryExpr:{ src1.temp > 20 } } }, \"}",
			t:   "FilterPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestHavingPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *HavingPlan
		res string
		t   string
	}{
		{
			p: &HavingPlan{
				condition: &ast.BinaryExpr{
					LHS: &ast.Call{Name: "count", FuncId: 0, Args: []ast.Expr{&ast.Wildcard{
						Token: ast.ASTERISK,
					}}, FuncType: ast.FuncTypeAgg},
					OP:  ast.GT,
					RHS: &ast.IntegerLiteral{Val: 2},
				},
			},
			res: `{"op":"HavingPlan_0","info":"Condition:{ binaryExpr:{ Call:{ name:count, args:[*] } > 2 } }, "}`,
			t:   "HavingPlan",
		},
		{
			p: &HavingPlan{
				condition: &ast.BinaryExpr{
					LHS: &ast.Call{
						Name:   "count",
						FuncId: 0,
						Args: []ast.Expr{
							&ast.Wildcard{
								Token: ast.ASTERISK,
								Replace: []ast.Field{
									{
										AName: "id1",
										Expr: &ast.BinaryExpr{
											OP: ast.MUL,
											LHS: &ast.FieldRef{
												Name:       "temp",
												StreamName: "src1",
											},
											RHS: &ast.IntegerLiteral{Val: 2},
										},
									},
									{
										AName: "name",
										Expr: &ast.BinaryExpr{
											OP: ast.MUL,
											LHS: &ast.FieldRef{
												Name:       "myarray",
												StreamName: "src1",
											},
											RHS: &ast.IntegerLiteral{Val: 2},
										},
									},
								},
							},
						},
						FuncType: ast.FuncTypeAgg,
					},
					OP:  ast.GT,
					RHS: &ast.IntegerLiteral{Val: 0},
				},
			},
			res: `{"op":"HavingPlan_1","info":"Condition:{ binaryExpr:{ Call:{ name:count, args:[*] } > 0 } }, "}`,
			t:   "HavingPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestJoinAlignPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *JoinAlignPlan
		res string
		t   string
	}{
		{
			p: &JoinAlignPlan{
				Emitters: []string{"tableInPlanner"},
			},
			res: `{"op":"JoinAlignPlan_0","info":"Emitters:[ tableInPlanner ]"}`,
			t:   "JoinAlignPlan",
		},
		{
			p: &JoinAlignPlan{
				Emitters: []string{"tableInPlanner"},
			},
			res: `{"op":"JoinAlignPlan_1","info":"Emitters:[ tableInPlanner ]"}`,
			t:   "JoinAlignPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestJoinPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *JoinPlan
		res string
		t   string
	}{
		{
			p: &JoinPlan{
				from: &ast.Table{Name: "src1"},
				joins: ast.Joins{ast.Join{
					Name:     "src2",
					JoinType: ast.INNER_JOIN,
					Expr: &ast.BinaryExpr{
						OP: ast.AND,
						LHS: &ast.BinaryExpr{
							LHS: &ast.BinaryExpr{
								OP:  ast.GT,
								LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
								RHS: &ast.IntegerLiteral{Val: 20},
							},
							OP: ast.OR,
							RHS: &ast.BinaryExpr{
								OP:  ast.GT,
								LHS: &ast.FieldRef{Name: "hum", StreamName: "src2"},
								RHS: &ast.IntegerLiteral{Val: 60},
							},
						},
						RHS: &ast.BinaryExpr{
							OP:  ast.EQ,
							LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
							RHS: &ast.FieldRef{Name: "id2", StreamName: "src2"},
						},
					},
				}},
			},
			res: `{"op":"JoinPlan_0","info":"Joins:[ { joinType:INNER_JOIN, binaryExpr:{ binaryExpr:{ binaryExpr:{ src1.temp > 20 } OR binaryExpr:{ src2.hum > 60 } } AND binaryExpr:{ src1.id1 = src2.id2 } } } ]"}`,
			t:   "JoinPlan",
		},
		{
			p: &JoinPlan{
				from: &ast.Table{Name: "src1"},
				joins: ast.Joins{ast.Join{
					Name:     "src2",
					JoinType: ast.INNER_JOIN,
					Expr: &ast.BinaryExpr{
						OP: ast.AND,
						LHS: &ast.BinaryExpr{
							LHS: &ast.BinaryExpr{
								OP:  ast.GT,
								LHS: &ast.FieldRef{Name: "cur", StreamName: "t1"},
								RHS: &ast.IntegerLiteral{Val: 20},
							},
							OP: ast.OR,
							RHS: &ast.BinaryExpr{
								OP:  ast.GT,
								LHS: &ast.FieldRef{Name: "pre", StreamName: "t2"},
								RHS: &ast.IntegerLiteral{Val: 60},
							},
						},
						RHS: &ast.BinaryExpr{
							OP:  ast.EQ,
							LHS: &ast.FieldRef{Name: "press1", StreamName: "t1"},
							RHS: &ast.FieldRef{Name: "press2", StreamName: "t2"},
						},
					},
				}},
			},
			res: `{"op":"JoinPlan_1","info":"Joins:[ { joinType:INNER_JOIN, binaryExpr:{ binaryExpr:{ binaryExpr:{ t1.cur > 20 } OR binaryExpr:{ t2.pre > 60 } } AND binaryExpr:{ t1.press1 = t2.press2 } } } ]"}`,
			t:   "JoinPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestLookupPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *LookupPlan
		res string
		t   string
	}{
		{
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
			res: `{"op":"LookupPlan_0","info":"Join:{ joinType:LEFT_JOIN, expr:binaryExpr:{ left.device_id = good.id } }"}`,
			t:   "LookupPlan",
		},
		{
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
			res: `{"op":"LookupPlan_1","info":"Join:{ joinType:LEFT_JOIN, expr:binaryExpr:{ left.device_id = 23 } }"}`,
			t:   "LookupPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestProjectPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *ProjectPlan
		res string
		t   string
	}{
		{
			p: &ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						HavingPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									FilterPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												WindowPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name:       "src1",
																isWildCard: true,
																streamFields: map[string]*ast.JsonStreamField{
																	"id1": {
																		Type: "bigint",
																	},
																	"temp": {
																		Type: "bigint",
																	},
																	"name": {
																		Type: "string",
																	},
																	"myarray": {
																		Type: "array",
																		Items: &ast.JsonStreamField{
																			Type: "string",
																		},
																	},
																},
																metaFields:  []string{},
																pruneFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.SLIDING_WINDOW,
													length:    10,
													timeUnit:  ast.SS,
													interval:  0,
													limit:     0,
												}.Init(),
											},
										},
										condition: &ast.BinaryExpr{
											LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
											OP:  ast.GT,
											RHS: &ast.IntegerLiteral{Val: 20},
										},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{Name: "count", FuncId: 0, Args: []ast.Expr{&ast.Wildcard{
									Token: ast.ASTERISK,
								}}, FuncType: ast.FuncTypeAgg},
								OP:  ast.GT,
								RHS: &ast.IntegerLiteral{Val: 2},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.Wildcard{Token: ast.ASTERISK},
						Name:  "*",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			},
			res: `{"op":"ProjectPlan_0","info":"Fields:[ * ]"}`,
			t:   "ProjectPlan",
		},
		{
			p: &ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"myarray": {
									Type: "array",
									Items: &ast.JsonStreamField{
										Type: "string",
									},
								},
							},
							metaFields:  []string{},
							pruneFields: []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name:  "unnest",
						AName: "col",
						Expr: func() *ast.FieldRef {
							fr := &ast.FieldRef{
								StreamName: ast.AliasStream,
								Name:       "col",
								AliasRef: &ast.AliasRef{
									Expression: &ast.Call{
										Name:     "unnest",
										FuncType: ast.FuncTypeSrf,
										Args: []ast.Expr{
											&ast.FieldRef{
												StreamName: "src1",
												Name:       "myarray",
											},
										},
									},
								},
							}
							fr.SetRefSource([]ast.StreamName{"src1"})
							return fr
						}(),
					},
				},
			},
			res: `{"op":"ProjectPlan_1","info":"Fields:[ $$alias.col,aliasRef:Call:{ name:unnest, args:[src1.myarray] } ]"}`,
			t:   "ProjectPlan",
		},
	}

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestProjectSetPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *ProjectSetPlan
		res string
		t   string
	}{
		{
			p: &ProjectSetPlan{
				SrfMapping: map[string]struct{}{
					"unnest": {},
				},
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						ProjectPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"myarray": {
												Type: "array",
												Items: &ast.JsonStreamField{
													Type: "string",
												},
											},
											"name": {
												Type: "string",
											},
										},
										metaFields:  []string{},
										pruneFields: []string{},
									}.Init(),
								},
							},
							fields: []ast.Field{
								{
									Expr: &ast.Call{
										Name:     "unnest",
										FuncType: ast.FuncTypeSrf,
										Args: []ast.Expr{
											&ast.FieldRef{
												StreamName: "src1",
												Name:       "myarray",
											},
										},
									},
									Name: "unnest",
								},
								{
									Name: "name",
									Expr: &ast.FieldRef{
										StreamName: "src1",
										Name:       "name",
									},
								},
							},
						}.Init(),
					},
				},
			},
			res: `{"op":"ProjectSetPlan_0","info":"SrfMap:{key:unnest}, EnableLimit:false"}`,
			t:   "ProjectSetPlan",
		},
	}

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestWindowPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *WindowPlan
		res string
		t   string
	}{
		{
			p: &WindowPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"name": {
												Type: "string",
											},
											"temp": {
												Type: "bigint",
											},
										},
										metaFields:  []string{},
										pruneFields: []string{},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.FieldRef{Name: "name", StreamName: "src1"},
								OP:  ast.EQ,
								RHS: &ast.StringLiteral{Val: "v1"},
							},
						}.Init(),
					},
				},
				condition: nil,
				wtype:     ast.TUMBLING_WINDOW,
				length:    10,
				timeUnit:  ast.SS,
				interval:  0,
				limit:     0,
			},
			res: `{"op":"WindowPlan_0","info":"{ length:10, windowType:TUMBLING_WINDOW, limit: 0 }"}`,
			t:   "WindowPlan",
		},
	}

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestWatermarkPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *WatermarkPlan
		res string
		t   string
	}{
		{
			p: &WatermarkPlan{
				Emitters:      []string{"id", "grade"},
				SendWatermark: false,
			},
			res: `{"op":"WatermarkPlan_0","info":"Emitters:[ id, grade ], SendWatermark:false"}`,
			t:   "WatermarkPlan",
		},
		{
			p: &WatermarkPlan{
				Emitters:      []string{"campus", "student"},
				SendWatermark: true,
			},
			res: `{"op":"WatermarkPlan_1","info":"Emitters:[ campus, student ], SendWatermark:true"}`,
			t:   "WatermarkPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestOrderPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *OrderPlan
		res string
		t   string
	}{
		{
			p: &OrderPlan{
				SortFields: []ast.SortField{{Uname: "name", Name: "name", Ascending: false, FieldExpr: &ast.FieldRef{Name: "name", StreamName: ast.DefaultStream}}},
			},
			res: `{"op":"OrderPlan_0","info":"SortFields:[ sortField:{ name:name, ascending:false, fieldExpr:{ $$default.name } } ]"}`,
			t:   "OrderPlan",
		},
		{
			p: &OrderPlan{
				SortFields: []ast.SortField{{Uname: "s1\007name", Name: "name", StreamName: ast.StreamName("s1"), Ascending: true, FieldExpr: &ast.FieldRef{Name: "name", StreamName: "s1"}}},
			},
			res: `{"op":"OrderPlan_1","info":"SortFields:[ sortField:{ name:name, ascending:true, fieldExpr:{ s1.name } } ]"}`,
			t:   "OrderPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		explainInfo := test[i].p.Explain()
		require.Equal(t, test[i].res, strings.Trim(explainInfo, "\n"))
	}
}

func TestSchemaInfo(t *testing.T) {
	schema.AddRuleSchema("r1", "d1", nil, true)
	p := DataSourcePlan{
		name: "d1",
	}.Init()
	require.Equal(t, " wildcard:true", p.buildSchemaInfo("r1"))
	schema.AddRuleSchema("r1", "d1", map[string]*ast.JsonStreamField{
		"a": {},
	}, false)
	require.Equal(t, " ConverterSchema:[a]", p.buildSchemaInfo("r1"))
}
