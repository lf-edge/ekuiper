package planner

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func Test_SlicePlan(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		err = kv.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(kv, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	cases := []struct {
		name string
		sql  string
		stmt *ast.SelectStatement
		err  string
	}{
		{
			name: "select fields",
			sql:  `SELECT a, b FROM src1`,
			stmt: &ast.SelectStatement{
				Fields: ast.Fields{
					ast.Field{
						Name:  "a",
						AName: "",
						Expr: &ast.FieldRef{
							StreamName:  "src1",
							Name:        "a",
							HasIndex:    true,
							SourceIndex: 0,
							Index:       0,
						},
					},
					ast.Field{
						Name:  "b",
						AName: "",
						Expr: &ast.FieldRef{
							StreamName:  "src1",
							Name:        "b",
							HasIndex:    true,
							SourceIndex: 1,
							Index:       1,
						},
					},
				},
				Sources: ast.Sources{
					&ast.Table{
						Name: "src1",
					},
				},
			},
		},
		{
			name: "alias, expression and filter",
			sql:  `SELECT a + b as ab, concat(c, d), b FROM src1 WHERE c > 5`,
			stmt: &ast.SelectStatement{
				Fields: ast.Fields{
					ast.Field{
						Name:  "",
						AName: "ab",
						Expr: &ast.FieldRef{
							StreamName:  "$$alias",
							Name:        "ab",
							HasIndex:    true,
							SourceIndex: -1,
							Index:       0,
							AliasRef: &ast.AliasRef{
								Expression: &ast.BinaryExpr{
									OP: ast.ADD,
									LHS: &ast.FieldRef{
										StreamName:  "src1",
										Name:        "a",
										HasIndex:    true,
										SourceIndex: 0,
										Index:       0,
									},
									RHS: &ast.FieldRef{
										StreamName:  "src1",
										Name:        "b",
										HasIndex:    true,
										SourceIndex: 1,
										Index:       0,
									},
								},
								RefSources: []ast.StreamName{
									"src1",
								},
							},
						},
					},
					ast.Field{
						Name:  "concat",
						AName: "",
						Expr: &ast.Call{
							Name: "concat",
							Args: []ast.Expr{
								&ast.FieldRef{
									StreamName:  "src1",
									Name:        "c",
									HasIndex:    true,
									SourceIndex: 2,
									Index:       1,
								},
								&ast.FieldRef{
									StreamName:  "src1",
									Name:        "d",
									HasIndex:    true,
									SourceIndex: 3,
									Index:       2,
								},
							},
						},
					},
					ast.Field{
						Name: "b",
						Expr: &ast.FieldRef{
							StreamName:  "src1",
							Name:        "b",
							HasIndex:    true,
							SourceIndex: 1,
							Index:       3,
						},
					},
				},
				Sources: ast.Sources{
					&ast.Table{
						Name: "src1",
					},
				},
				Condition: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.FieldRef{
						StreamName:  "src1",
						Name:        "c",
						HasIndex:    true,
						SourceIndex: 2,
						Index:       0,
					},
					RHS: &ast.IntegerLiteral{
						Val: int64(5),
					},
				},
			},
		},
		{
			name: "analytic func in filter",
			sql:  `SELECT lag(a) as la, a FROM src1 WHERE latest(b) > 5`,
			stmt: &ast.SelectStatement{
				Fields: ast.Fields{
					ast.Field{
						Name:  "lag",
						AName: "la",
						Expr: &ast.FieldRef{
							StreamName:  "$$alias",
							Name:        "la",
							HasIndex:    true,
							SourceIndex: -1,
							Index:       0,
							AliasRef: &ast.AliasRef{
								Expression: &ast.Call{
									Name: "lag",
									Args: []ast.Expr{
										&ast.FieldRef{
											StreamName:  "src1",
											Name:        "a",
											HasIndex:    true,
											SourceIndex: 0,
											Index:       0,
										},
									},
									CacheIndex:  0,
									CachedField: "$$a_lag_0",
									Cached:      true,
								},
								RefSources: []ast.StreamName{
									"src1",
								},
							},
						},
					},
					ast.Field{
						Name: "a",
						Expr: &ast.FieldRef{
							StreamName:  "src1",
							Name:        "a",
							HasIndex:    true,
							SourceIndex: 0,
							Index:       1,
						},
					},
				},
				Sources: ast.Sources{
					&ast.Table{
						Name: "src1",
					},
				},
				Condition: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.Call{
						Name:   "latest",
						FuncId: 1,
						Args: []ast.Expr{
							&ast.FieldRef{
								StreamName:  "src1",
								Name:        "b",
								HasIndex:    true,
								SourceIndex: 1,
								Index:       0,
							},
						},
						CacheIndex:  1,
						CachedField: "$$a_latest_1",
						Cached:      true,
					},
					RHS: &ast.IntegerLiteral{
						Val: int64(5),
					},
				},
			},
		},
		{
			name: "window",
			sql:  `SELECT a FROM src1 GROUP BY SlidingWindow(ss, 2) OVER (WHEN b > 5)`,
			stmt: &ast.SelectStatement{
				Fields: ast.Fields{
					ast.Field{
						Name: "a",
						Expr: &ast.FieldRef{
							StreamName:  "src1",
							Name:        "a",
							HasIndex:    true,
							SourceIndex: 0,
							Index:       0,
						},
					},
				},
				Sources: ast.Sources{
					&ast.Table{
						Name: "src1",
					},
				},
				Dimensions: ast.Dimensions{
					ast.Dimension{
						Expr: &ast.Window{
							TriggerCondition: &ast.BinaryExpr{
								OP: ast.GT,
								LHS: &ast.FieldRef{
									StreamName:  "src1",
									Name:        "b",
									HasIndex:    true,
									SourceIndex: 1,
									Index:       0,
								},
								RHS: &ast.IntegerLiteral{
									Val: int64(5),
								},
							},
							WindowType: ast.SLIDING_WINDOW,
							Delay: &ast.IntegerLiteral{
								Val: int64(0),
							},
							Length: &ast.IntegerLiteral{
								Val: int64(2),
							},
							Interval: &ast.IntegerLiteral{
								Val: int64(0),
							},
							TimeUnit: &ast.TimeLiteral{
								Val: ast.SS,
							},
						},
					},
				},
			},
		},
		{
			name: "use invisible alias",
			sql:  `SELECT a + b as ab invisible, concat(c, d) as cd, b, concat(cd, ab) as cde FROM src1 WHERE c > 5`,
			stmt: &ast.SelectStatement{
				Fields: ast.Fields{
					ast.Field{
						Name:      "",
						AName:     "ab",
						Invisible: true,
						Expr: &ast.FieldRef{
							StreamName:  "$$alias",
							Name:        "ab",
							HasIndex:    true,
							SourceIndex: -1,
							Index:       3,
							AliasRef: &ast.AliasRef{
								Expression: &ast.BinaryExpr{
									OP: ast.ADD,
									LHS: &ast.FieldRef{
										StreamName:  "src1",
										Name:        "a",
										HasIndex:    true,
										SourceIndex: 0,
										Index:       0,
									},
									RHS: &ast.FieldRef{
										StreamName:  "src1",
										Name:        "b",
										HasIndex:    true,
										SourceIndex: 1,
										Index:       0,
									},
								},
								RefSources: []ast.StreamName{
									"src1",
								},
							},
						},
					},
					ast.Field{
						Name:  "concat",
						AName: "cd",
						Expr: &ast.FieldRef{
							StreamName:  "$$alias",
							Name:        "cd",
							HasIndex:    true,
							SourceIndex: -1,
							Index:       0,
							AliasRef: &ast.AliasRef{
								Expression: &ast.Call{
									Name: "concat",
									Args: []ast.Expr{
										&ast.FieldRef{
											StreamName:  "src1",
											Name:        "c",
											HasIndex:    true,
											SourceIndex: 2,
											Index:       0,
										},
										&ast.FieldRef{
											StreamName:  "src1",
											Name:        "d",
											HasIndex:    true,
											SourceIndex: 3,
											Index:       0,
										},
									},
								},
								RefSources: []ast.StreamName{
									"src1",
								},
							},
						},
					},
					ast.Field{
						Name: "b",
						Expr: &ast.FieldRef{
							StreamName:  "src1",
							Name:        "b",
							HasIndex:    true,
							SourceIndex: 1,
							Index:       1,
						},
					},
					ast.Field{
						Name:  "concat",
						AName: "cde",
						Expr: &ast.FieldRef{
							StreamName:  "$$alias",
							Name:        "cde",
							HasIndex:    true,
							SourceIndex: -1,
							Index:       2,
							AliasRef: &ast.AliasRef{
								Expression: &ast.Call{
									Name:   "concat",
									FuncId: 1,
									Args: []ast.Expr{
										&ast.FieldRef{
											StreamName:  "$$alias",
											Name:        "cd",
											HasIndex:    true,
											SourceIndex: -1,
											Index:       0,
											AliasRef: &ast.AliasRef{
												Expression: &ast.Call{
													Name: "concat",
													Args: []ast.Expr{
														&ast.FieldRef{
															StreamName:  "src1",
															Name:        "c",
															HasIndex:    true,
															SourceIndex: 2,
															Index:       0,
														},
														&ast.FieldRef{
															StreamName:  "src1",
															Name:        "d",
															HasIndex:    true,
															SourceIndex: 3,
															Index:       0,
														},
													},
												},
												RefSources: []ast.StreamName{
													"src1",
												},
											},
										},
										&ast.FieldRef{
											StreamName:  "$$alias",
											Name:        "ab",
											HasIndex:    true,
											SourceIndex: -1,
											Index:       3,
											AliasRef: &ast.AliasRef{
												Expression: &ast.BinaryExpr{
													OP: ast.ADD,
													LHS: &ast.FieldRef{
														StreamName:  "src1",
														Name:        "a",
														HasIndex:    true,
														SourceIndex: 0,
														Index:       0,
													},
													RHS: &ast.FieldRef{
														StreamName:  "src1",
														Name:        "b",
														HasIndex:    true,
														SourceIndex: 1,
														Index:       0,
													},
												},
												RefSources: []ast.StreamName{
													"src1",
												},
											},
										},
									},
								},
								RefSources: []ast.StreamName{
									"src1",
								},
							},
						},
					},
				},
				Sources: ast.Sources{
					&ast.Table{
						Name: "src1",
					},
				},
				Condition: &ast.BinaryExpr{
					OP: ast.GT,
					LHS: &ast.FieldRef{
						StreamName:  "src1",
						Name:        "c",
						HasIndex:    true,
						SourceIndex: 2,
						Index:       0,
					},
					RHS: &ast.IntegerLiteral{
						Val: int64(5),
					},
				},
			},
		},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rule := &def.Rule{
				Id:        "tt" + strconv.Itoa(i),
				Triggered: false,
				Sql:       tt.sql,
				Actions: []map[string]any{
					{"log": map[string]any{}},
				},
				Options: &def.RuleOption{
					IsEventTime: false,
					Experiment:  &def.ExpOpts{UseSliceTuple: true},
				},
			}
			_, stmt, err := PlanSQLWithSourcesAndSinks(rule, nil)
			require.NoError(t, err)
			require.Equal(t, tt.stmt, stmt)
		})
	}
}
