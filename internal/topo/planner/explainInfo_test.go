package planner

import (
	"testing"

	"github.com/lf-edge/ekuiper/pkg/ast"
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
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test1, Fields:[ field1, field2, field3 ]\",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test2, StreamFields:[ a, b, c ]\",\"id\":1,\"children\":[0]}\n",
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
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test3, Fields:[ column1, column2, id ], StreamFields:[ s1, s2, s3 ]\",\"id\":2,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test4",
			},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test4\",\"id\":3,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
		{
			p:   &DataSourcePlan{},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"\",\"id\":4,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[2].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.self.SetID(int64(i))
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
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
			res: "{\"type\":\"AggregatePlan\",\"info\":\"Dimension:{ Call:{ name:lpad, args:[$$default.name, 1] } }\",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"AggregatePlan\",\"info\":\"Dimension:{ window:{ windowType:SLIDING_WINDOW, timeUnit: MS  } }\",\"id\":0,\"children\":[0]}\n",
			t:   "AggregatePlan",
		},
		{
			p: &AggregatePlan{
				dimensions: ast.Dimensions{
					ast.Dimension{Expr: &ast.FieldRef{Name: "department", StreamName: ast.DefaultStream}},
				},
			},
			res: "{\"type\":\"AggregatePlan\",\"info\":\"Dimension:{ $$default.department }\",\"id\":0,\"children\":null}\n",
			t:   "AggregatePlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[2].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"AnalyticFuncsPlan\",\"info\":\"Funcs:[ Call:{ name:lag, args:[src1.temp] } ], FieldFuncs:[ Call:{ name:lag, args:[src1.name] }, Call:{ name:latest, args:[Call:{ name:lag, args:[src1.name] }] } ]\",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"AnalyticFuncsPlan\",\"info\":\"FieldFuncs:[ Call:{ name:lag, args:[src1.id1] }, Call:{ name:lag, args:[src1.temp], when:{ binaryExpr:{ Call:{ name:lag, args:[src1.id1] } > 1 } } } ]\",\"id\":0,\"children\":[0]}\n",
			t:   "AnalyticFuncsPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"FilterPlan\",\"info\":\"Condition:{ binaryExpr:{ src1.name = v1 } }, \",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"FilterPlan\",\"info\":\"Condition:{ binaryExpr:{ binaryExpr:{ src1.id1 > 111 } AND binaryExpr:{ src1.temp > 20 } } }, \",\"id\":0,\"children\":[0]}\n",
			t:   "FilterPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"HavingPlan\",\"info\":\"Condition:{ binaryExpr:{ Call:{ name:count, args:[*] } > 2 } }, \",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"HavingPlan\",\"info\":\"Condition:{ binaryExpr:{ Call:{ name:count, args:[*] } > 0 } }, \",\"id\":0,\"children\":[0]}\n",
			t:   "HavingPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"JoinAlignPlan\",\"info\":\"Emitters:[ tableInPlanner ]\",\"id\":0,\"children\":null}\n",
			t:   "JoinAlignPlan",
		},
		{
			p: &JoinAlignPlan{
				Emitters: []string{"tableInPlanner"},
			},
			res: "{\"type\":\"JoinAlignPlan\",\"info\":\"Emitters:[ tableInPlanner ]\",\"id\":0,\"children\":[0]}\n",
			t:   "JoinAlignPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"JoinPlan\",\"info\":\"Joins:[ { joinType:INNER_JOIN, binaryExpr:{ binaryExpr:{ binaryExpr:{ src1.temp > 20 } OR binaryExpr:{ src2.hum > 60 } } AND binaryExpr:{ src1.id1 = src2.id2 } } } ]\",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"JoinPlan\",\"info\":\"Joins:[ { joinType:INNER_JOIN, binaryExpr:{ binaryExpr:{ binaryExpr:{ t1.cur > 20 } OR binaryExpr:{ t2.pre > 60 } } AND binaryExpr:{ t1.press1 = t2.press2 } } } ]\",\"id\":0,\"children\":[0]}\n",
			t:   "JoinPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"LookupPlan\",\"info\":\"Join:{ joinType:LEFT_JOIN, expr:binaryExpr:{ left.device_id = good.id } }\",\"id\":0,\"children\":null}\n",
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
			res: "{\"type\":\"LookupPlan\",\"info\":\"Join:{ joinType:LEFT_JOIN, expr:binaryExpr:{ left.device_id = 23 } }\",\"id\":0,\"children\":[0]}\n",
			t:   "LookupPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"ProjectPlan\",\"info\":\"\",\"id\":0,\"children\":[0]}\n",
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
			res: "{\"type\":\"ProjectPlan\",\"info\":\"Fields:[ $$alias.col ]\",\"id\":0,\"children\":[0]}\n",
			t:   "ProjectPlan",
		},
	}

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[1].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"ProjectSetPlan\",\"info\":\"SrfMap:{key:unnest}, EnableLimit:false\",\"id\":0,\"children\":[0]}\n",
			t:   "ProjectSetPlan",
		},
	}

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"WindowPlan\",\"info\":\"{ length:10, windowType:TUMBLING_WINDOW, limit: 0 }\",\"id\":0,\"children\":[0]}\n",
			t:   "WindowPlan",
		},
	}

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"WatermarkPlan\",\"info\":\"Emitters:[ id, grade ], SendWatermark:false\",\"id\":0,\"children\":null}\n",
			t:   "WatermarkPlan",
		},
		{
			p: &WatermarkPlan{
				Emitters:      []string{"campus", "student"},
				SendWatermark: true,
			},
			res: "{\"type\":\"WatermarkPlan\",\"info\":\"Emitters:[ campus, student ], SendWatermark:true\",\"id\":0,\"children\":[0]}\n",
			t:   "WatermarkPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
	}
}

func TestWindowFuncPlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *WindowFuncPlan
		res string
		t   string
	}{
		{
			p: &WindowFuncPlan{
				windowFuncFields: ast.Fields{
					{
						Name:  "Name",
						AName: "AName",
						Expr:  &ast.IndexExpr{},
					},
					{
						Name:  "Name",
						AName: "AName",
						Expr:  &ast.IndexExpr{},
					},
					{
						Name:  "Name",
						AName: "",
						Expr:  nil,
					},
					{
						Name:  "",
						AName: "AName",
						Expr:  nil,
					},
				},
			},
			res: "{\"type\":\"WindowFuncPlan\",\"info\":\"windowFuncFields:[ {name:AName, expr:}, {name:AName, expr:}, {name:Name}, {name:AName} ]\",\"id\":0,\"children\":null}\n",
			t:   "WindowFuncPlan",
		},
		{
			p: &WindowFuncPlan{
				windowFuncFields: ast.Fields{
					{
						Name:  "Student",
						AName: "AStudent",
						Expr:  &ast.IndexExpr{},
					},
				},
			},
			res: "{\"type\":\"WindowFuncPlan\",\"info\":\"windowFuncFields:[ {name:AStudent, expr:} ]\",\"id\":0,\"children\":[0]}\n",
			t:   "WindowFuncPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
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
			res: "{\"type\":\"OrderPlan\",\"info\":\"SortFields:[ sortField:{ name:name, ascending:false, fieldExpr:{ $$default.name } } ]\",\"id\":0,\"children\":null}\n",
			t:   "OrderPlan",
		},
		{
			p: &OrderPlan{
				SortFields: []ast.SortField{{Uname: "s1\007name", Name: "name", StreamName: ast.StreamName("s1"), Ascending: true, FieldExpr: &ast.FieldRef{Name: "name", StreamName: "s1"}}},
			},
			res: "{\"type\":\"OrderPlan\",\"info\":\"SortFields:[ sortField:{ name:name, ascending:true, fieldExpr:{ s1.name } } ]\",\"id\":0,\"children\":[0]}\n",
			t:   "OrderPlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[0].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo()
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
	}
}
