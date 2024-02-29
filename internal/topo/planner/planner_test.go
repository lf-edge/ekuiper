// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gdexlab/go-render/render"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/mqtt"
	"github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	nodeConf "github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func init() {
	testx.InitEnv("planner")
}

var defaultOption = &api.RuleOption{
	IsEventTime:        false,
	LateTol:            1000,
	Concurrency:        1,
	BufferLength:       1024,
	SendMetaToSink:     false,
	SendError:          true,
	Qos:                api.AtMostOnce,
	CheckpointInterval: 300000,
	Restart: &api.RestartStrategy{
		Attempts:     0,
		Delay:        1000,
		Multiplier:   2,
		MaxDelay:     30000,
		JitterFactor: 0.1,
	},
}

func Test_createLogicalPlan(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					myarray array(string)
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"src2": `CREATE STREAM src2 (
					id2 BIGINT,
					hum BIGINT
				) WITH (DATASOURCE="src2", FORMAT="json", KEY="ts", TIMESTAMP_FORMAT="YYYY-MM-dd HH:mm:ss");`,
		"tableInPlanner": `CREATE TABLE tableInPlanner (
					id BIGINT,
					name STRING,
					value STRING,
					hum BIGINT
				) WITH (TYPE="file");`,
		"src3": `CREATE STREAM src3 (
					a struct(b struct(c bigint,d bigint),e bigint),
					a1 struct(b struct(c bigint,d bigint),e bigint),
                    a2 bigint
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1":           ast.TypeStream,
		"src2":           ast.TypeStream,
		"src3":           ast.TypeStream,
		"tableInPlanner": ast.TypeTable,
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

	// boolTrue = true
	boolFalse := false

	ref := &ast.AliasRef{
		Expression: &ast.Call{
			Name:     "row_number",
			FuncType: ast.FuncTypeWindow,
		},
	}
	ref.SetRefSource([]string{})

	srcHumRef := &ast.AliasRef{
		Expression: &ast.FieldRef{
			StreamName: "src2",
			Name:       "hum",
		},
	}
	srcHumRef.SetRefSource([]string{"src2"})

	tableHumRef := &ast.AliasRef{
		Expression: &ast.FieldRef{
			StreamName: "tableInPlanner",
			Name:       "hum",
		},
	}
	tableHumRef.SetRefSource([]string{"tableInPlanner"})

	arrowCRef := &ast.AliasRef{
		Expression: &ast.BinaryExpr{
			OP: ast.ARROW,
			LHS: &ast.BinaryExpr{
				OP: ast.ARROW,
				LHS: &ast.FieldRef{
					StreamName: "src3",
					Name:       "a",
				},
				RHS: &ast.JsonFieldRef{
					Name: "b",
				},
			},
			RHS: &ast.JsonFieldRef{
				Name: "c",
			},
		},
	}
	arrowCRef.SetRefSource([]string{"src3"})
	arrowPRef := &ast.AliasRef{
		Expression: &ast.BinaryExpr{
			OP: ast.ARROW,
			LHS: &ast.FieldRef{
				StreamName: "src3",
				Name:       "a",
			},
			RHS: &ast.JsonFieldRef{
				Name: "e",
			},
		},
	}
	arrowPRef.SetRefSource([]string{"src3"})
	tests := []struct {
		sql string
		p   LogicalPlan
		err string
	}{
		{
			sql: "select a.b.c as c, a.e as e, a2 from src3",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src3",
							streamFields: map[string]*ast.JsonStreamField{
								"a": {
									Type: "struct",
									Properties: map[string]*ast.JsonStreamField{
										"b": {
											Type: "struct",
											Properties: map[string]*ast.JsonStreamField{
												"c": {
													Type: "bigint",
												},
											},
										},
										"e": {
											Type: "bigint",
										},
									},
								},
								"a2": {
									Type: "bigint",
								},
							},
							streamStmt:  streams["src3"],
							metaFields:  []string{},
							pruneFields: []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name:  "",
						AName: "c",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "c",
							AliasRef:   arrowCRef,
						},
					},
					{
						Name:  "e",
						AName: "e",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "e",
							AliasRef:   arrowPRef,
						},
					},
					{
						Name: "a2",
						Expr: &ast.FieldRef{
							StreamName: "src3",
							Name:       "a2",
						},
					},
				},
			}.Init(),
		},
		{
			sql: "select src2.hum as hum, tableInPlanner.hum as hum2 from src2 left join tableInPlanner on src2.hum = tableInPlanner.hum",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						//
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													pruneFields:     []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src2",
							},
							joins: ast.Joins{ast.Join{
								Name:     "tableInPlanner",
								JoinType: ast.LEFT_JOIN,
								Expr: &ast.BinaryExpr{
									OP: ast.EQ,
									LHS: &ast.FieldRef{
										StreamName: "src2",
										Name:       "hum",
									},
									RHS: &ast.FieldRef{
										StreamName: "tableInPlanner",
										Name:       "hum",
									},
								},
							}},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name:  "hum",
						AName: "hum",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "hum",
							AliasRef:   srcHumRef,
						},
					},
					{
						Name:  "hum",
						AName: "hum2",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "hum2",
							AliasRef:   tableHumRef,
						},
					},
				},
			}.Init(),
		},
		{
			sql: "select name, row_number() as index from src1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowFuncPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"name": {
												Type: "string",
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{},
										pruneFields: []string{},
									}.Init(),
								},
							},
							windowFuncFields: []ast.Field{
								{
									Name:  "row_number",
									AName: "index",
									Expr: &ast.FieldRef{
										StreamName: ast.AliasStream,
										Name:       "index",
										AliasRef:   ref,
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name:  "row_number",
						AName: "index",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "index",
							AliasRef:   ref,
						},
					},
					{
						Name: "name",
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "name",
						},
					},
				},
				windowFuncNames: map[string]struct{}{
					"index": {},
				},
			}.Init(),
		},
		{
			sql: "select name, row_number() from src1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowFuncPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"name": {
												Type: "string",
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{},
										pruneFields: []string{},
									}.Init(),
								},
							},
							windowFuncFields: []ast.Field{
								{
									Name: "row_number",
									Expr: &ast.Call{
										Name:     "row_number",
										FuncType: ast.FuncTypeWindow,
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "name",
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "name",
						},
					},
					{
						Name: "row_number",
						Expr: &ast.Call{
							Name:     "row_number",
							FuncType: ast.FuncTypeWindow,
						},
					},
				},
				windowFuncNames: map[string]struct{}{
					"row_number": {},
				},
			}.Init(),
		},
		{
			sql: "select name from src1 where true limit 1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"name": {
												Type: "string",
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{},
										pruneFields: []string{},
									}.Init(),
								},
							},
							condition: &ast.BooleanLiteral{
								Val: true,
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "name",
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "name",
						},
					},
				},
				limitCount:  1,
				enableLimit: true,
			}.Init(),
		},
		{
			sql: "select name from src1 limit 1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"name": {
									Type: "string",
								},
							},
							streamStmt:  streams["src1"],
							metaFields:  []string{},
							pruneFields: []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "name",
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "name",
						},
					},
				},
				limitCount:  1,
				enableLimit: true,
			}.Init(),
		},
		{
			sql: "select unnest(myarray) as col from src1 limit 1",
			p: ProjectSetPlan{
				SrfMapping: map[string]struct{}{
					"col": {},
				},
				limitCount:  1,
				enableLimit: true,
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
										},
										streamStmt:  streams["src1"],
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
						}.Init(),
					},
				},
			}.Init(),
		},
		{ // 0
			sql: "SELECT unnest(myarray), name from src1",
			p: ProjectSetPlan{
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
										streamStmt:  streams["src1"],
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
			}.Init(),
		},
		{ // 0
			sql: `SELECT myarray[temp] FROM src1`,
			p: ProjectPlan{
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
								"temp": {
									Type: "bigint",
								},
							},
							streamStmt:  streams["src1"],
							metaFields:  []string{},
							pruneFields: []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.BinaryExpr{
							OP: ast.SUBSET,
							LHS: &ast.FieldRef{
								StreamName: "src1",
								Name:       "myarray",
							},
							RHS: &ast.IndexExpr{Index: &ast.FieldRef{
								StreamName: "src1",
								Name:       "temp",
							}},
						},
						Name:  "kuiper_field_0",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 1 optimize where to data source
			sql: `SELECT temp FROM src1 WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
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
													streamStmt:  streams["src1"],
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 2 condition that cannot be optimized
			sql: `SELECT id1 FROM src1 INNER JOIN src2 on src1.id1 = src2.id2 WHERE src1.temp > 20 OR src2.hum > 60 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1": {
															Type: "bigint",
														},
														"temp": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id2": {
															Type: "bigint",
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
													pruneFields:     []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 3 optimize window filter
			sql: `SELECT id1 FROM src1 WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10) FILTER( WHERE temp > 2)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									FilterPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1": {
															Type: "bigint",
														},
														"name": {
															Type: "string",
														},
														"temp": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										condition: &ast.BinaryExpr{
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												LHS: &ast.FieldRef{Name: "name", StreamName: "src1"},
												OP:  ast.EQ,
												RHS: &ast.StringLiteral{Val: "v1"},
											},
											RHS: &ast.BinaryExpr{
												LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
												OP:  ast.GT,
												RHS: &ast.IntegerLiteral{Val: 2},
											},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 4. do not optimize count window
			sql: `SELECT * FROM src1 WHERE temp > 20 GROUP BY COUNTWINDOW(5,1) HAVING COUNT(*) > 2`,
			p: ProjectPlan{
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
																streamStmt:  streams["src1"],
																metaFields:  []string{},
																pruneFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.COUNT_WINDOW,
													length:    5,
													interval:  1,
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
			}.Init(),
		},
		{ // 5. optimize join on
			sql: `SELECT id1 FROM src1 INNER JOIN src2 on src1.id1 = src2.id2 and src1.temp > 20 and src2.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1": {
																		Type: "bigint",
																	},
																	"temp": {
																		Type: "bigint",
																	},
																},
																streamStmt:  streams["src1"],
																metaFields:  []string{},
																pruneFields: []string{},
															}.Init(),
														},
													},
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
												}.Init(),
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src2",
																streamFields: map[string]*ast.JsonStreamField{
																	"hum": {
																		Type: "bigint",
																	},
																	"id2": {
																		Type: "bigint",
																	},
																},
																streamStmt:      streams["src2"],
																metaFields:      []string{},
																timestampFormat: "YYYY-MM-dd HH:mm:ss",
																pruneFields:     []string{},
															}.Init(),
														},
													},
													condition: &ast.BinaryExpr{
														OP:  ast.LT,
														LHS: &ast.FieldRef{Name: "hum", StreamName: "src2"},
														RHS: &ast.IntegerLiteral{Val: 60},
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
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "src2",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
										OP:  ast.EQ,
										RHS: &ast.FieldRef{Name: "id2", StreamName: "src2"},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 6. optimize outter join on
			sql: `SELECT id1 FROM src1 FULL JOIN src2 on src1.id1 = src2.id2 and src1.temp > 20 and src2.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1": {
																		Type: "bigint",
																	},
																	"temp": {
																		Type: "bigint",
																	},
																},
																streamStmt:  streams["src1"],
																metaFields:  []string{},
																pruneFields: []string{},
															}.Init(),
														},
													},
													condition: &ast.BinaryExpr{
														OP:  ast.GT,
														LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
														RHS: &ast.IntegerLiteral{Val: 111},
													},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id2": {
															Type: "bigint",
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													pruneFields:     []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "src2",
									Alias:    "",
									JoinType: ast.FULL_JOIN,
									Expr: &ast.BinaryExpr{
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
												OP:  ast.EQ,
												RHS: &ast.FieldRef{Name: "id2", StreamName: "src2"},
											},
											RHS: &ast.BinaryExpr{
												OP:  ast.GT,
												LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
												RHS: &ast.IntegerLiteral{Val: 20},
											},
										},
										RHS: &ast.BinaryExpr{
											OP:  ast.LT,
											LHS: &ast.FieldRef{Name: "hum", StreamName: "src2"},
											RHS: &ast.IntegerLiteral{Val: 60},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 7 window error for table
			sql: `SELECT value FROM tableInPlanner WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p:   nil,
			err: "cannot run window for TABLE sources",
		},
		{ // 8 join table without window
			sql: `SELECT id1 FROM src1 INNER JOIN tableInPlanner on src1.id1 = tableInPlanner.id and src1.temp > 20 and hum < 60 WHERE src1.id1 > 111`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1": {
																		Type: "bigint",
																	},
																	"temp": {
																		Type: "bigint",
																	},
																},
																streamStmt:  streams["src1"],
																metaFields:  []string{},
																pruneFields: []string{},
															}.Init(),
														},
													},
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
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "tableInPlanner",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
											OP:  ast.EQ,
											RHS: &ast.FieldRef{Name: "id", StreamName: "tableInPlanner"},
										},
										RHS: &ast.BinaryExpr{
											OP:  ast.LT,
											LHS: &ast.FieldRef{Name: "hum", StreamName: "tableInPlanner"},
											RHS: &ast.IntegerLiteral{Val: 60},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 9 join table with window
			sql: `SELECT id1 FROM src1 INNER JOIN tableInPlanner on src1.id1 = tableInPlanner.id and src1.temp > 20 and tableInPlanner.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												WindowPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1": {
																		Type: "bigint",
																	},
																	"temp": {
																		Type: "bigint",
																	},
																},
																streamStmt:  streams["src1"],
																metaFields:  []string{},
																pruneFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.TUMBLING_WINDOW,
													length:    10,
													timeUnit:  ast.SS,
													interval:  0,
													limit:     0,
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "tableInPlanner",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
											OP:  ast.EQ,
											RHS: &ast.FieldRef{Name: "id", StreamName: "tableInPlanner"},
										},
										RHS: &ast.BinaryExpr{
											RHS: &ast.BinaryExpr{
												OP: ast.AND,
												LHS: &ast.BinaryExpr{
													OP:  ast.GT,
													LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
													RHS: &ast.IntegerLiteral{Val: 20},
												},
												RHS: &ast.BinaryExpr{
													OP:  ast.LT,
													LHS: &ast.FieldRef{Name: "hum", StreamName: "tableInPlanner"},
													RHS: &ast.IntegerLiteral{Val: 60},
												},
											},
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												OP:  ast.GT,
												LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
												RHS: &ast.IntegerLiteral{Val: 111},
											},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 10 meta
			sql: `SELECT temp, meta(id) AS eid,meta(Humidity->Device) AS hdevice FROM src1 WHERE meta(device)="demo2"`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"temp": {
												Type: "bigint",
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{"Humidity", "device", "id"},
										pruneFields: []string{},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "meta",
									FuncId: 2,
									Args: []ast.Expr{&ast.MetaRef{
										Name:       "device",
										StreamName: ast.DefaultStream,
									}},
								},
								OP: ast.EQ,
								RHS: &ast.StringLiteral{
									Val: "demo2",
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{Name: "eid", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.Call{Name: "meta", FuncId: 0, Args: []ast.Expr{&ast.MetaRef{
								Name:       "id",
								StreamName: ast.DefaultStream,
							}}},
							[]ast.StreamName{},
							nil,
						)},
						Name:  "meta",
						AName: "eid",
					},
					{
						Expr: &ast.FieldRef{Name: "hdevice", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.Call{Name: "meta", FuncId: 1, Args: []ast.Expr{
								&ast.BinaryExpr{
									OP:  ast.ARROW,
									LHS: &ast.MetaRef{Name: "Humidity", StreamName: ast.DefaultStream},
									RHS: &ast.JsonFieldRef{Name: "Device"},
								},
							}},
							[]ast.StreamName{},
							nil,
						)},
						Name:  "meta",
						AName: "hdevice",
					},
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 11 join with same name field and aliased
			sql: `SELECT src2.hum AS hum1, tableInPlanner.hum AS hum2 FROM src2 INNER JOIN tableInPlanner on id2 = id WHERE hum1 > hum2`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id2": {
															Type: "bigint",
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
													pruneFields:     []string{},
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src2",
							},
							joins: []ast.Join{
								{
									Name:     "tableInPlanner",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										RHS: &ast.BinaryExpr{
											OP:  ast.EQ,
											LHS: &ast.FieldRef{Name: "id2", StreamName: "src2"},
											RHS: &ast.FieldRef{Name: "id", StreamName: "tableInPlanner"},
										},
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											OP: ast.GT,
											LHS: &ast.FieldRef{Name: "hum1", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
												&ast.FieldRef{
													Name:       "hum",
													StreamName: "src2",
												},
												[]ast.StreamName{"src2"},
												&boolFalse,
											)},
											RHS: &ast.FieldRef{Name: "hum2", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
												&ast.FieldRef{
													Name:       "hum",
													StreamName: "tableInPlanner",
												},
												[]ast.StreamName{"tableInPlanner"},
												&boolFalse,
											)},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{Name: "hum1", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.FieldRef{
								Name:       "hum",
								StreamName: "src2",
							},
							[]ast.StreamName{"src2"},
							&boolFalse,
						)},
						Name:  "hum",
						AName: "hum1",
					}, {
						Expr: &ast.FieldRef{Name: "hum2", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.FieldRef{
								Name:       "hum",
								StreamName: "tableInPlanner",
							},
							[]ast.StreamName{"tableInPlanner"},
							&boolFalse,
						)},
						Name:  "hum",
						AName: "hum2",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 12 meta with more fields
			sql: `SELECT temp, meta(*) as m FROM src1 WHERE meta(device)="demo2"`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"temp": {
												Type: "bigint",
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{},
										allMeta:     true,
										pruneFields: []string{},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "meta",
									FuncId: 1,
									Args: []ast.Expr{&ast.MetaRef{
										Name:       "device",
										StreamName: ast.DefaultStream,
									}},
								},
								OP: ast.EQ,
								RHS: &ast.StringLiteral{
									Val: "demo2",
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{Name: "m", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.Call{Name: "meta", FuncId: 0, Args: []ast.Expr{&ast.MetaRef{
								Name:       "*",
								StreamName: ast.DefaultStream,
							}}},
							[]ast.StreamName{},
							nil,
						)},
						Name:  "meta",
						AName: "m",
					},
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 13 analytic function plan
			sql: `SELECT latest(lag(name)), id1 FROM src1 WHERE lag(temp) > temp`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									AnalyticFuncsPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1": {
															Type: "bigint",
														},
														"name": {
															Type: "string",
														},
														"temp": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
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
												Name: "latest", FuncId: 1, CachedField: "$$a_latest_1", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}},
											},
										},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "lag",
									FuncId: 2,
									Args: []ast.Expr{&ast.FieldRef{
										Name:       "temp",
										StreamName: "src1",
									}},
									CachedField: "$$a_lag_2",
									Cached:      true,
								},
								OP: ast.GT,
								RHS: &ast.FieldRef{
									Name:       "temp",
									StreamName: "src1",
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.Call{
							Name:        "latest",
							FuncId:      1,
							Args:        []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}},
							CachedField: "$$a_latest_1",
							Cached:      true,
						},
						Name: "latest",
					}, {
						Expr: &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name: "id1",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 14
			sql: `SELECT name, *, meta(device) FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
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
							streamStmt:  streams["src1"],
							metaFields:  []string{"device"},
							isWildCard:  true,
							pruneFields: []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "name", StreamName: "src1"},
						Name:  "name",
						AName: "",
					},
					{
						Name: "*",
						Expr: &ast.Wildcard{
							Token: ast.ASTERISK,
						},
					},
					{
						Name: "meta",
						Expr: &ast.Call{
							Name: "meta",
							Args: []ast.Expr{
								&ast.MetaRef{
									StreamName: ast.DefaultStream,
									Name:       "device",
								},
							},
						},
					},
				},
				isAggregate: false,
				allWildcard: true,
				sendMeta:    false,
			}.Init(),
		},
		{ // 15 analytic function over partition plan
			sql: `SELECT latest(lag(name)) OVER (PARTITION BY temp), id1 FROM src1 WHERE latest(lag(temp)) > temp`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									AnalyticFuncsPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1": {
															Type: "bigint",
														},
														"name": {
															Type: "string",
														},
														"temp": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
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
											{
												Name:        "latest",
												FuncId:      3,
												CachedField: "$$a_latest_3",
												Args: []ast.Expr{&ast.Call{
													Name:        "lag",
													FuncId:      2,
													CachedField: "$$a_lag_2",
													Cached:      true,
													Args: []ast.Expr{&ast.FieldRef{
														Name:       "temp",
														StreamName: "src1",
													}},
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
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:        "latest",
									FuncId:      3,
									CachedField: "$$a_latest_3",
									Cached:      true,
									Args: []ast.Expr{&ast.Call{
										Name:        "lag",
										FuncId:      2,
										CachedField: "$$a_lag_2",
										Args: []ast.Expr{&ast.FieldRef{
											Name:       "temp",
											StreamName: "src1",
										}},
										Cached: true,
									}},
								},
								OP: ast.GT,
								RHS: &ast.FieldRef{
									Name:       "temp",
									StreamName: "src1",
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.Call{
							Name:        "latest",
							FuncId:      1,
							Args:        []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}},
							CachedField: "$$a_latest_1",
							Cached:      true,
							Partition: &ast.PartitionExpr{
								Exprs: []ast.Expr{
									&ast.FieldRef{Name: "temp", StreamName: "src1"},
								},
							},
						},
						Name: "latest",
					}, {
						Expr: &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name: "id1",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 16 analytic function over partition when plan
			sql: `SELECT latest(lag(name)) OVER (PARTITION BY temp WHEN temp > 12), CASE id1 WHEN 1 THEN lag(id1) END FROM src1 WHERE lag(temp) > temp`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									AnalyticFuncsPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1": {
															Type: "bigint",
														},
														"name": {
															Type: "string",
														},
														"temp": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										funcs: []*ast.Call{
											{
												Name:        "lag",
												FuncId:      3,
												CachedField: "$$a_lag_3",
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
												Name: "latest", FuncId: 1, CachedField: "$$a_latest_1", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}}, Partition: &ast.PartitionExpr{Exprs: []ast.Expr{&ast.FieldRef{Name: "temp", StreamName: "src1"}}}, WhenExpr: &ast.BinaryExpr{LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"}, OP: ast.GT, RHS: &ast.IntegerLiteral{Val: 12}},
											},
											{
												Name: "lag", FuncId: 2, CachedField: "$$a_lag_2", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "id1", StreamName: "src1"}},
											},
										},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "lag",
									FuncId: 3,
									Args: []ast.Expr{&ast.FieldRef{
										Name:       "temp",
										StreamName: "src1",
									}},
									CachedField: "$$a_lag_3",
									Cached:      true,
								},
								OP: ast.GT,
								RHS: &ast.FieldRef{
									Name:       "temp",
									StreamName: "src1",
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.Call{
							Name:        "latest",
							FuncId:      1,
							Args:        []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}},
							CachedField: "$$a_latest_1",
							Cached:      true,
							Partition: &ast.PartitionExpr{
								Exprs: []ast.Expr{
									&ast.FieldRef{Name: "temp", StreamName: "src1"},
								},
							},
							WhenExpr: &ast.BinaryExpr{
								LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
								OP:  ast.GT,
								RHS: &ast.IntegerLiteral{Val: 12},
							},
						},
						Name: "latest",
					}, {
						Expr: &ast.CaseExpr{
							WhenClauses: []*ast.WhenClause{
								{
									Expr: &ast.IntegerLiteral{Val: 1},
									Result: &ast.Call{
										Name:        "lag",
										FuncId:      2,
										FuncType:    ast.FuncTypeScalar,
										Args:        []ast.Expr{&ast.FieldRef{Name: "id1", StreamName: "src1"}},
										CachedField: "$$a_lag_2",
										Cached:      true,
									},
								},
							},
							Value: &ast.FieldRef{
								StreamName: "src1",
								Name:       "id1",
							},
						},
						Name: "kuiper_field_0",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 17. do not optimize sliding window
			sql: `SELECT * FROM src1 WHERE temp > 20 GROUP BY SLIDINGWINDOW(ss, 10) HAVING COUNT(*) > 2`,
			p: ProjectPlan{
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
																streamStmt:  streams["src1"],
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
			}.Init(),
		},
		{
			// 18 analytic function over when plan
			sql: `SELECT CASE WHEN lag(temp) OVER (WHEN lag(id1) > 1) BETWEEN 0 AND 10 THEN 1 ELSE 0 END FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						AnalyticFuncsPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"id1": {
												Type: "bigint",
											},
											"temp": {
												Type: "bigint",
											},
										},
										streamStmt: &ast.StreamStmt{
											Name: "src1",
											StreamFields: []ast.StreamField{
												{
													Name: "id1",
													FieldType: &ast.BasicType{
														Type: ast.DataType(1),
													},
												},
												{
													Name: "temp",
													FieldType: &ast.BasicType{
														Type: ast.DataType(1),
													},
												},
												{
													Name: "name",
													FieldType: &ast.BasicType{
														Type: ast.DataType(3),
													},
												},
												{
													Name: "myarray",
													FieldType: &ast.ArrayType{
														Type: ast.DataType(3),
													},
												},
											},
											Options: &ast.Options{
												DATASOURCE: "src1",
												KEY:        "ts",
												FORMAT:     "json",
											},
											StreamType: ast.StreamType(0),
										},
										metaFields:  []string{},
										pruneFields: []string{},
									}.Init(),
								},
							},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "kuiper_field_0",
						Expr: &ast.CaseExpr{
							WhenClauses: []*ast.WhenClause{
								{
									Expr: &ast.BinaryExpr{
										OP: ast.BETWEEN,
										LHS: &ast.Call{
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
											Cached:      true,
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
										RHS: &ast.BetweenExpr{
											Lower: &ast.IntegerLiteral{
												Val: 0,
											},
											Higher: &ast.IntegerLiteral{
												Val: 10,
											},
										},
									},
									Result: &ast.IntegerLiteral{
										Val: 1,
									},
								},
							},
							ElseClause: &ast.IntegerLiteral{
								Val: 0,
							},
						},
					},
				},
			}.Init(),
		},
		{ // 19
			sql: `SELECT * EXCEPT(id1, name), meta(device) FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"temp": {
									Type: "bigint",
								},
								"myarray": {
									Type: "array",
									Items: &ast.JsonStreamField{
										Type: "string",
									},
								},
							},
							streamStmt:  streams["src1"],
							metaFields:  []string{"device"},
							isWildCard:  false,
							pruneFields: []string{"id1", "name"},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "*",
						Expr: &ast.Wildcard{
							Token:  ast.ASTERISK,
							Except: []string{"id1", "name"},
						},
					},
					{
						Name: "meta",
						Expr: &ast.Call{
							Name: "meta",
							Args: []ast.Expr{
								&ast.MetaRef{
									StreamName: ast.DefaultStream,
									Name:       "device",
								},
							},
						},
					},
				},
				isAggregate: false,
				allWildcard: true,
				sendMeta:    false,
			}.Init(),
		},
		{ // 20
			sql: `SELECT * REPLACE(temp * 2 AS id1, myarray * 2 AS name), meta(device) FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"temp": {
									Type: "bigint",
								},
								"myarray": {
									Type: "array",
									Items: &ast.JsonStreamField{
										Type: "string",
									},
								},
							},
							streamStmt:  streams["src1"],
							metaFields:  []string{"device"},
							isWildCard:  false,
							pruneFields: []string{"id1", "name"},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "*",
						Expr: &ast.Wildcard{
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
					{
						Name: "meta",
						Expr: &ast.Call{
							Name: "meta",
							Args: []ast.Expr{
								&ast.MetaRef{
									StreamName: ast.DefaultStream,
									Name:       "device",
								},
							},
						},
					},
				},
				isAggregate: false,
				allWildcard: true,
				sendMeta:    false,
			}.Init(),
		},
		{ // 21
			sql: `SELECT collect( * EXCEPT(id1, name)) FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"temp": {
												Type: "bigint",
											},
											"myarray": {
												Type: "array",
												Items: &ast.JsonStreamField{
													Type: "string",
												},
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{},
										isWildCard:  false,
										pruneFields: []string{"id1", "name"},
									}.Init(),
								},
							},
							condition: nil,
							wtype:     ast.TUMBLING_WINDOW,
							length:    10,
							timeUnit:  ast.SS,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "collect",
						Expr: &ast.Call{
							Name:     "collect",
							FuncType: ast.FuncTypeAgg,
							Args: []ast.Expr{
								&ast.Wildcard{
									Token:  ast.ASTERISK,
									Except: []string{"id1", "name"},
								},
							},
						},
					},
				},
				isAggregate: true,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 22
			sql: `SELECT collect( * REPLACE(temp * 2 AS id1, myarray * 2 AS name)) FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"temp": {
												Type: "bigint",
											},
											"myarray": {
												Type: "array",
												Items: &ast.JsonStreamField{
													Type: "string",
												},
											},
										},
										streamStmt:  streams["src1"],
										metaFields:  []string{},
										isWildCard:  false,
										pruneFields: []string{"id1", "name"},
									}.Init(),
								},
							},
							condition: nil,
							wtype:     ast.TUMBLING_WINDOW,
							length:    10,
							timeUnit:  ast.SS,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "collect",
						Expr: &ast.Call{
							Name:     "collect",
							FuncType: ast.FuncTypeAgg,
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
						},
					},
				},
				isAggregate: true,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 23
			sql: `SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) HAVING count(* EXCEPT(id1, name)) > 0`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						HavingPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													baseLogicalPlan: baseLogicalPlan{},
													name:            "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1": {
															Type: "bigint",
														},
														"temp": {
															Type: "bigint",
														},
														"myarray": {
															Type: "array",
															Items: &ast.JsonStreamField{
																Type: "string",
															},
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													isWildCard:  false,
													pruneFields: []string{"id1", "name"},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "count",
									FuncId: 0,
									Args: []ast.Expr{
										&ast.Wildcard{
											Token:  ast.ASTERISK,
											Except: []string{"id1", "name"},
										},
									},
									FuncType: ast.FuncTypeAgg,
								},
								OP:  ast.GT,
								RHS: &ast.IntegerLiteral{Val: 0},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "id1",
						Expr: &ast.FieldRef{
							Name:       "id1",
							StreamName: "src1",
						},
					},
				},
				isAggregate: false,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 24
			sql: `SELECT temp FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) HAVING count(* REPLACE(temp * 2 AS id1, myarray * 2 AS name)) > 0`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						HavingPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													baseLogicalPlan: baseLogicalPlan{},
													name:            "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"temp": {
															Type: "bigint",
														},
														"myarray": {
															Type: "array",
															Items: &ast.JsonStreamField{
																Type: "string",
															},
														},
													},
													streamStmt:  streams["src1"],
													metaFields:  []string{},
													isWildCard:  false,
													pruneFields: []string{"id1", "name"},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "temp",
						Expr: &ast.FieldRef{
							Name:       "temp",
							StreamName: "src1",
						},
					},
				},
				isAggregate: false,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		p, err := createLogicalPlan(stmt, &api.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, kv)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %v: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		} else {
			ok := assert.Equal(t, tt.p, p, "%d plan mismatch %s", i)
			if !ok {
				t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, render.AsCode(tt.p), render.AsCode(p))
			}
		}
	}
}

func Test_createLogicalPlanSchemaless(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"src2": `CREATE STREAM src2 (
				) WITH (DATASOURCE="src2", FORMAT="json", KEY="ts");`,
		"tableInPlanner": `CREATE TABLE tableInPlanner (
					id BIGINT,
					name STRING,
					value STRING,
					hum BIGINT
				) WITH (TYPE="file");`,
	}
	types := map[string]ast.StreamType{
		"src1":           ast.TypeStream,
		"src2":           ast.TypeStream,
		"tableInPlanner": ast.TypeTable,
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

	// boolTrue = true
	boolFalse := false

	tests := []struct {
		sql string
		p   LogicalPlan
		err string
	}{
		{ // 0
			sql: `SELECT name FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"name": nil,
							},
							streamStmt:   streams["src1"],
							isSchemaless: true,
							metaFields:   []string{},
							pruneFields:  []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "name", StreamName: "src1"},
						Name:  "name",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 1 optimize where to data source
			sql: `SELECT temp FROM src1 WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									FilterPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"name": nil,
														"temp": nil,
													},
													streamStmt:   streams["src1"],
													metaFields:   []string{},
													isSchemaless: true,
													pruneFields:  []string{},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 2 condition that cannot be optimized
			sql: `SELECT id1 FROM src1 INNER JOIN src2 on src1.id1 = src2.id2 WHERE src1.temp > 20 OR src2.hum > 60 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1":  nil,
														"temp": nil,
													},
													streamStmt:   streams["src1"],
													metaFields:   []string{},
													isSchemaless: true,
													pruneFields:  []string{},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{ // can't determine where is id1 belonged to
														"hum": nil,
														"id1": nil,
														"id2": nil,
													},
													isSchemaless: true,
													streamStmt:   streams["src2"],
													metaFields:   []string{},
													pruneFields:  []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: ast.DefaultStream},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 3 optimize window filter
			sql: `SELECT id1 FROM src1 WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10) FILTER( WHERE temp > 2)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									FilterPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"id1":  nil,
														"name": nil,
														"temp": nil,
													},
													isSchemaless: true,
													streamStmt:   streams["src1"],
													metaFields:   []string{},
													pruneFields:  []string{},
												}.Init(),
											},
										},
										condition: &ast.BinaryExpr{
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												LHS: &ast.FieldRef{Name: "name", StreamName: "src1"},
												OP:  ast.EQ,
												RHS: &ast.StringLiteral{Val: "v1"},
											},
											RHS: &ast.BinaryExpr{
												LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
												OP:  ast.GT,
												RHS: &ast.IntegerLiteral{Val: 2},
											},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 4. do not optimize count window
			sql: `SELECT * FROM src1 WHERE temp > 20 GROUP BY COUNTWINDOW(5,1) HAVING COUNT(*) > 2`,
			p: ProjectPlan{
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
																name:         "src1",
																isWildCard:   true,
																streamFields: map[string]*ast.JsonStreamField{},
																streamStmt:   streams["src1"],
																metaFields:   []string{},
																isSchemaless: true,
																pruneFields:  []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.COUNT_WINDOW,
													length:    5,
													interval:  1,
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
			}.Init(),
		},
		{ // 5. optimize join on
			sql: `SELECT id1 FROM src1 INNER JOIN src2 on src1.id1 = src2.id2 and src1.temp > 20 and src2.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1":  nil,
																	"temp": nil,
																},
																isSchemaless: true,
																streamStmt:   streams["src1"],
																metaFields:   []string{},
																pruneFields:  []string{},
															}.Init(),
														},
													},
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
												}.Init(),
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src2",
																streamFields: map[string]*ast.JsonStreamField{
																	"hum": nil,
																	"id1": nil,
																	"id2": nil,
																},
																isSchemaless: true,
																streamStmt:   streams["src2"],
																metaFields:   []string{},
																pruneFields:  []string{},
															}.Init(),
														},
													},
													condition: &ast.BinaryExpr{
														OP:  ast.LT,
														LHS: &ast.FieldRef{Name: "hum", StreamName: "src2"},
														RHS: &ast.IntegerLiteral{Val: 60},
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
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "src2",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
										OP:  ast.EQ,
										RHS: &ast.FieldRef{Name: "id2", StreamName: "src2"},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: ast.DefaultStream},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 6. optimize outter join on
			sql: `SELECT id1 FROM src1 FULL JOIN src2 on src1.id1 = src2.id2 and src1.temp > 20 and src2.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1":  nil,
																	"temp": nil,
																},
																isSchemaless: true,
																streamStmt:   streams["src1"],
																metaFields:   []string{},
																pruneFields:  []string{},
															}.Init(),
														},
													},
													condition: &ast.BinaryExpr{
														OP:  ast.GT,
														LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
														RHS: &ast.IntegerLiteral{Val: 111},
													},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": nil,
														"id1": nil,
														"id2": nil,
													},
													isSchemaless: true,
													streamStmt:   streams["src2"],
													metaFields:   []string{},
													pruneFields:  []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "src2",
									Alias:    "",
									JoinType: ast.FULL_JOIN,
									Expr: &ast.BinaryExpr{
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
												OP:  ast.EQ,
												RHS: &ast.FieldRef{Name: "id2", StreamName: "src2"},
											},
											RHS: &ast.BinaryExpr{
												OP:  ast.GT,
												LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
												RHS: &ast.IntegerLiteral{Val: 20},
											},
										},
										RHS: &ast.BinaryExpr{
											OP:  ast.LT,
											LHS: &ast.FieldRef{Name: "hum", StreamName: "src2"},
											RHS: &ast.IntegerLiteral{Val: 60},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: ast.DefaultStream},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 7 window error for table
			sql: `SELECT value FROM tableInPlanner WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p:   nil,
			err: "cannot run window for TABLE sources",
		},
		{ // 8 join table without window
			sql: `SELECT id1 FROM src1 INNER JOIN tableInPlanner on src1.id1 = tableInPlanner.id and src1.temp > 20 and hum < 60 WHERE src1.id1 > 111`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"hum":  nil,
																	"id1":  nil,
																	"temp": nil,
																},
																isSchemaless: true,
																streamStmt:   streams["src1"],
																metaFields:   []string{},
																pruneFields:  []string{},
															}.Init(),
														},
													},
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
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "tableInPlanner",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
											OP:  ast.EQ,
											RHS: &ast.FieldRef{Name: "id", StreamName: "tableInPlanner"},
										},
										RHS: &ast.BinaryExpr{
											OP:  ast.LT,
											LHS: &ast.FieldRef{Name: "hum", StreamName: ast.DefaultStream},
											RHS: &ast.IntegerLiteral{Val: 60},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: ast.DefaultStream},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 9 join table with window
			sql: `SELECT id1 FROM src1 INNER JOIN tableInPlanner on src1.id1 = tableInPlanner.id and src1.temp > 20 and tableInPlanner.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												WindowPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"id1":  nil,
																	"temp": nil,
																},
																isSchemaless: true,
																streamStmt:   streams["src1"],
																metaFields:   []string{},
																pruneFields:  []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.TUMBLING_WINDOW,
													length:    10,
													timeUnit:  ast.SS,
													interval:  0,
													limit:     0,
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src1",
							},
							joins: []ast.Join{
								{
									Name:     "tableInPlanner",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
											OP:  ast.EQ,
											RHS: &ast.FieldRef{Name: "id", StreamName: "tableInPlanner"},
										},
										RHS: &ast.BinaryExpr{
											RHS: &ast.BinaryExpr{
												OP: ast.AND,
												LHS: &ast.BinaryExpr{
													OP:  ast.GT,
													LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"},
													RHS: &ast.IntegerLiteral{Val: 20},
												},
												RHS: &ast.BinaryExpr{
													OP:  ast.LT,
													LHS: &ast.FieldRef{Name: "hum", StreamName: "tableInPlanner"},
													RHS: &ast.IntegerLiteral{Val: 60},
												},
											},
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												OP:  ast.GT,
												LHS: &ast.FieldRef{Name: "id1", StreamName: "src1"},
												RHS: &ast.IntegerLiteral{Val: 111},
											},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: ast.DefaultStream},
						Name:  "id1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 10 meta
			sql: `SELECT temp, meta(id) AS eid,meta(Humidity->Device) AS hdevice FROM src1 WHERE meta(device)="demo2"`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"temp": nil,
										},
										isSchemaless: true,
										streamStmt:   streams["src1"],
										metaFields:   []string{"Humidity", "device", "id"},
										pruneFields:  []string{},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "meta",
									FuncId: 2,
									Args: []ast.Expr{&ast.MetaRef{
										Name:       "device",
										StreamName: ast.DefaultStream,
									}},
								},
								OP: ast.EQ,
								RHS: &ast.StringLiteral{
									Val: "demo2",
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: "",
					}, {
						Expr: &ast.FieldRef{Name: "eid", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.Call{Name: "meta", FuncId: 0, Args: []ast.Expr{&ast.MetaRef{
								Name:       "id",
								StreamName: ast.DefaultStream,
							}}},
							[]ast.StreamName{},
							nil,
						)},
						Name:  "meta",
						AName: "eid",
					}, {
						Expr: &ast.FieldRef{Name: "hdevice", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.Call{Name: "meta", FuncId: 1, Args: []ast.Expr{
								&ast.BinaryExpr{
									OP:  ast.ARROW,
									LHS: &ast.MetaRef{Name: "Humidity", StreamName: ast.DefaultStream},
									RHS: &ast.JsonFieldRef{Name: "Device"},
								},
							}},
							[]ast.StreamName{},
							nil,
						)},
						Name:  "meta",
						AName: "hdevice",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 11 join with same name field and aliased
			sql: `SELECT src2.hum AS hum1, tableInPlanner.hum AS hum2 FROM src2 INNER JOIN tableInPlanner on id2 = id WHERE hum1 > hum2`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						JoinPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinAlignPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													name: "src2",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": nil,
														"id":  nil,
														"id2": nil,
													},
													isSchemaless: true,
													streamStmt:   streams["src2"],
													metaFields:   []string{},
													pruneFields:  []string{},
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: map[string]*ast.JsonStreamField{
														"hum": {
															Type: "bigint",
														},
														"id": {
															Type: "bigint",
														},
													},
													streamStmt:  streams["tableInPlanner"],
													metaFields:  []string{},
													pruneFields: []string{},
												}.Init(),
											},
										},
										Emitters: []string{"tableInPlanner"},
									}.Init(),
								},
							},
							from: &ast.Table{
								Name: "src2",
							},
							joins: []ast.Join{
								{
									Name:     "tableInPlanner",
									Alias:    "",
									JoinType: ast.INNER_JOIN,
									Expr: &ast.BinaryExpr{
										RHS: &ast.BinaryExpr{
											OP:  ast.EQ,
											LHS: &ast.FieldRef{Name: "id2", StreamName: ast.DefaultStream},
											RHS: &ast.FieldRef{Name: "id", StreamName: ast.DefaultStream},
										},
										OP: ast.AND,
										LHS: &ast.BinaryExpr{
											OP: ast.GT,
											LHS: &ast.FieldRef{Name: "hum1", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
												&ast.FieldRef{
													Name:       "hum",
													StreamName: "src2",
												},
												[]ast.StreamName{"src2"},
												&boolFalse,
											)},
											RHS: &ast.FieldRef{Name: "hum2", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
												&ast.FieldRef{
													Name:       "hum",
													StreamName: "tableInPlanner",
												},
												[]ast.StreamName{"tableInPlanner"},
												&boolFalse,
											)},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{Name: "hum1", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.FieldRef{
								Name:       "hum",
								StreamName: "src2",
							},
							[]ast.StreamName{"src2"},
							&boolFalse,
						)},
						Name:  "hum",
						AName: "hum1",
					},
					{
						Expr: &ast.FieldRef{Name: "hum2", StreamName: ast.AliasStream, AliasRef: ast.MockAliasRef(
							&ast.FieldRef{
								Name:       "hum",
								StreamName: "tableInPlanner",
							},
							[]ast.StreamName{"tableInPlanner"},
							&boolFalse,
						)},
						Name:  "hum",
						AName: "hum2",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 12
			sql: `SELECT name->first, name->last FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"name": nil,
							},
							isSchemaless: true,
							streamStmt:   streams["src1"],
							metaFields:   []string{},
							pruneFields:  []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.BinaryExpr{
							OP:  ast.ARROW,
							LHS: &ast.FieldRef{StreamName: "src1", Name: "name"},
							RHS: &ast.JsonFieldRef{Name: "first"},
						},
						Name:  "kuiper_field_0",
						AName: "",
					}, {
						Expr: &ast.BinaryExpr{
							OP:  ast.ARROW,
							LHS: &ast.FieldRef{StreamName: "src1", Name: "name"},
							RHS: &ast.JsonFieldRef{Name: "last"},
						},
						Name:  "kuiper_field_1",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 13
			sql: `SELECT * EXCEPT(id1, name), meta(device) FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields:    map[string]*ast.JsonStreamField{},
							streamStmt:      streams["src1"],
							metaFields:      []string{"device"},
							isWildCard:      false,
							pruneFields:     []string{"id1", "name"},
							isSchemaless:    true,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "*",
						Expr: &ast.Wildcard{
							Token:  ast.ASTERISK,
							Except: []string{"id1", "name"},
						},
					},
					{
						Name: "meta",
						Expr: &ast.Call{
							Name: "meta",
							Args: []ast.Expr{
								&ast.MetaRef{
									StreamName: ast.DefaultStream,
									Name:       "device",
								},
							},
						},
					},
				},
				isAggregate: false,
				allWildcard: true,
				sendMeta:    false,
			}.Init(),
		},
		{ // 14
			sql: `SELECT * REPLACE(temp * 2 AS id1, myarray * 2 AS name), meta(device) FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields:    map[string]*ast.JsonStreamField{},
							streamStmt:      streams["src1"],
							metaFields:      []string{"device"},
							isWildCard:      false,
							pruneFields:     []string{"id1", "name"},
							isSchemaless:    true,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "*",
						Expr: &ast.Wildcard{
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
					{
						Name: "meta",
						Expr: &ast.Call{
							Name: "meta",
							Args: []ast.Expr{
								&ast.MetaRef{
									StreamName: ast.DefaultStream,
									Name:       "device",
								},
							},
						},
					},
				},
				isAggregate: false,
				allWildcard: true,
				sendMeta:    false,
			}.Init(),
		},
		{ // 15
			sql: `SELECT collect( * EXCEPT(id1, name)) FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields:    map[string]*ast.JsonStreamField{},
										streamStmt:      streams["src1"],
										metaFields:      []string{},
										isWildCard:      false,
										pruneFields:     []string{"id1", "name"},
										isSchemaless:    true,
									}.Init(),
								},
							},
							condition: nil,
							wtype:     ast.TUMBLING_WINDOW,
							length:    10,
							timeUnit:  ast.SS,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "collect",
						Expr: &ast.Call{
							Name:     "collect",
							FuncType: ast.FuncTypeAgg,
							Args: []ast.Expr{
								&ast.Wildcard{
									Token:  ast.ASTERISK,
									Except: []string{"id1", "name"},
								},
							},
						},
					},
				},
				isAggregate: true,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 16
			sql: `SELECT collect( * REPLACE(temp * 2 AS id1, myarray * 2 AS name)) FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						WindowPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields:    map[string]*ast.JsonStreamField{},
										streamStmt:      streams["src1"],
										metaFields:      []string{},
										isWildCard:      false,
										pruneFields:     []string{"id1", "name"},
										isSchemaless:    true,
									}.Init(),
								},
							},
							condition: nil,
							wtype:     ast.TUMBLING_WINDOW,
							length:    10,
							timeUnit:  ast.SS,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "collect",
						Expr: &ast.Call{
							Name:     "collect",
							FuncType: ast.FuncTypeAgg,
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
						},
					},
				},
				isAggregate: true,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 17
			sql: `SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) HAVING count(* EXCEPT(id1, name)) > 0`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						HavingPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													baseLogicalPlan: baseLogicalPlan{},
													name:            "src1",
													streamFields:    map[string]*ast.JsonStreamField{},
													streamStmt:      streams["src1"],
													metaFields:      []string{},
													isWildCard:      false,
													pruneFields:     []string{"id1", "name"},
													isSchemaless:    true,
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								LHS: &ast.Call{
									Name:   "count",
									FuncId: 0,
									Args: []ast.Expr{
										&ast.Wildcard{
											Token:  ast.ASTERISK,
											Except: []string{"id1", "name"},
										},
									},
									FuncType: ast.FuncTypeAgg,
								},
								OP:  ast.GT,
								RHS: &ast.IntegerLiteral{Val: 0},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "id1",
						Expr: &ast.FieldRef{
							Name:       "id1",
							StreamName: "src1",
						},
					},
				},
				isAggregate: false,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 18
			sql: `SELECT temp FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) HAVING count(* REPLACE(temp * 2 AS id1, myarray * 2 AS name)) > 0`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						HavingPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									WindowPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													baseLogicalPlan: baseLogicalPlan{},
													name:            "src1",
													streamFields:    map[string]*ast.JsonStreamField{},
													streamStmt:      streams["src1"],
													metaFields:      []string{},
													isWildCard:      false,
													pruneFields:     []string{"id1", "name"},
													isSchemaless:    true,
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10,
										timeUnit:  ast.SS,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
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
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Name: "temp",
						Expr: &ast.FieldRef{
							Name:       "temp",
							StreamName: "src1",
						},
					},
				},
				isAggregate: false,
				allWildcard: false,
				sendMeta:    false,
			}.Init(),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		p, err := createLogicalPlan(stmt, &api.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, kv)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		} else if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %v\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, render.AsCode(tt.p), render.AsCode(p))
		}
	}
}

func Test_createLogicalPlan4Lookup(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1":   `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"table1": `CREATE TABLE table1 () WITH (DATASOURCE="table1",TYPE="sql", KIND="lookup");`,
		"table2": `CREATE TABLE table2 () WITH (DATASOURCE="table2",TYPE="sql", KIND="lookup");`,
	}
	types := map[string]ast.StreamType{
		"src1":   ast.TypeStream,
		"table1": ast.TypeTable,
		"table2": ast.TypeTable,
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
	tests := []struct {
		sql string
		p   LogicalPlan
		err string
	}{
		{ // 0
			sql: `SELECT src1.a, table1.b FROM src1 INNER JOIN table1 ON src1.id = table1.id`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						LookupPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										baseLogicalPlan: baseLogicalPlan{},
										name:            "src1",
										streamFields: map[string]*ast.JsonStreamField{
											"a":  nil,
											"id": nil,
										},
										isSchemaless: true,
										streamStmt:   streams["src1"],
										metaFields:   []string{},
										pruneFields:  []string{},
									}.Init(),
								},
							},
							joinExpr: ast.Join{
								Name:     "table1",
								Alias:    "",
								JoinType: ast.INNER_JOIN,
								Expr: &ast.BinaryExpr{
									OP: ast.EQ,
									LHS: &ast.FieldRef{
										StreamName: "src1",
										Name:       "id",
									},
									RHS: &ast.FieldRef{
										StreamName: "table1",
										Name:       "id",
									},
								},
							},
							keys:   []string{"id"},
							fields: []string{"b"},
							valvars: []ast.Expr{
								&ast.FieldRef{
									StreamName: "src1",
									Name:       "id",
								},
							},
							options: &ast.Options{
								DATASOURCE: "table1",
								TYPE:       "sql",
								KIND:       "lookup",
							},
							conditions: nil,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "a",
						},
						Name:  "a",
						AName: "",
					},
					{
						Expr: &ast.FieldRef{
							StreamName: "table1",
							Name:       "b",
						},
						Name:  "b",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 1
			sql: `SELECT src1.a, table1.* FROM src1 INNER JOIN table1 ON table1.b > 20 AND src1.c < 40 AND src1.id = table1.id`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									LookupPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																baseLogicalPlan: baseLogicalPlan{},
																name:            "src1",
																streamFields: map[string]*ast.JsonStreamField{
																	"a":  nil,
																	"c":  nil,
																	"id": nil,
																},
																isSchemaless: true,
																streamStmt:   streams["src1"],
																metaFields:   []string{},
																pruneFields:  []string{},
															}.Init(),
														},
													},
													condition: &ast.BinaryExpr{
														OP: ast.LT,
														LHS: &ast.FieldRef{
															StreamName: "src1",
															Name:       "c",
														},
														RHS: &ast.IntegerLiteral{Val: 40},
													},
												}.Init(),
											},
										},
										joinExpr: ast.Join{
											Name:     "table1",
											Alias:    "",
											JoinType: ast.INNER_JOIN,
											Expr: &ast.BinaryExpr{
												OP: ast.AND,
												RHS: &ast.BinaryExpr{
													OP: ast.EQ,
													LHS: &ast.FieldRef{
														StreamName: "src1",
														Name:       "id",
													},
													RHS: &ast.FieldRef{
														StreamName: "table1",
														Name:       "id",
													},
												},
												LHS: &ast.BinaryExpr{
													OP: ast.AND,
													LHS: &ast.BinaryExpr{
														OP: ast.GT,
														LHS: &ast.FieldRef{
															StreamName: "table1",
															Name:       "b",
														},
														RHS: &ast.IntegerLiteral{Val: 20},
													},
													RHS: &ast.BinaryExpr{
														OP: ast.LT,
														LHS: &ast.FieldRef{
															StreamName: "src1",
															Name:       "c",
														},
														RHS: &ast.IntegerLiteral{Val: 40},
													},
												},
											},
										},
										keys: []string{"id"},
										valvars: []ast.Expr{
											&ast.FieldRef{
												StreamName: "src1",
												Name:       "id",
											},
										},
										options: &ast.Options{
											DATASOURCE: "table1",
											TYPE:       "sql",
											KIND:       "lookup",
										},
										conditions: &ast.BinaryExpr{
											OP: ast.AND,
											LHS: &ast.BinaryExpr{
												OP: ast.GT,
												LHS: &ast.FieldRef{
													StreamName: "table1",
													Name:       "b",
												},
												RHS: &ast.IntegerLiteral{Val: 20},
											},
											RHS: &ast.BinaryExpr{
												OP: ast.LT,
												LHS: &ast.FieldRef{
													StreamName: "src1",
													Name:       "c",
												},
												RHS: &ast.IntegerLiteral{Val: 40},
											},
										},
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								OP: ast.GT,
								LHS: &ast.FieldRef{
									StreamName: "table1",
									Name:       "b",
								},
								RHS: &ast.IntegerLiteral{Val: 20},
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "a",
						},
						Name:  "a",
						AName: "",
					},
					{
						Expr: &ast.FieldRef{
							StreamName: "table1",
							Name:       "*",
						},
						Name:  "*",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 2
			sql: `SELECT src1.a, table1.b, table2.c FROM src1 INNER JOIN table1 ON src1.id = table1.id INNER JOIN table2 on table1.id = table2.id`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						LookupPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									LookupPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												DataSourcePlan{
													baseLogicalPlan: baseLogicalPlan{},
													name:            "src1",
													streamFields: map[string]*ast.JsonStreamField{
														"a":  nil,
														"id": nil,
													},
													isSchemaless: true,
													streamStmt:   streams["src1"],
													metaFields:   []string{},
													pruneFields:  []string{},
												}.Init(),
											},
										},
										joinExpr: ast.Join{
											Name:     "table1",
											Alias:    "",
											JoinType: ast.INNER_JOIN,
											Expr: &ast.BinaryExpr{
												OP: ast.EQ,
												LHS: &ast.FieldRef{
													StreamName: "src1",
													Name:       "id",
												},
												RHS: &ast.FieldRef{
													StreamName: "table1",
													Name:       "id",
												},
											},
										},
										keys:   []string{"id"},
										fields: []string{"b", "id"},
										valvars: []ast.Expr{
											&ast.FieldRef{
												StreamName: "src1",
												Name:       "id",
											},
										},
										options: &ast.Options{
											DATASOURCE: "table1",
											TYPE:       "sql",
											KIND:       "lookup",
										},
										conditions: nil,
									}.Init(),
								},
							},
							joinExpr: ast.Join{
								Name:     "table2",
								Alias:    "",
								JoinType: ast.INNER_JOIN,
								Expr: &ast.BinaryExpr{
									OP: ast.EQ,
									LHS: &ast.FieldRef{
										StreamName: "table1",
										Name:       "id",
									},
									RHS: &ast.FieldRef{
										StreamName: "table2",
										Name:       "id",
									},
								},
							},
							keys:   []string{"id"},
							fields: []string{"c"},
							valvars: []ast.Expr{
								&ast.FieldRef{
									StreamName: "table1",
									Name:       "id",
								},
							},
							options: &ast.Options{
								DATASOURCE: "table2",
								TYPE:       "sql",
								KIND:       "lookup",
							},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr: &ast.FieldRef{
							StreamName: "src1",
							Name:       "a",
						},
						Name:  "a",
						AName: "",
					},
					{
						Expr: &ast.FieldRef{
							StreamName: "table1",
							Name:       "b",
						},
						Name:  "b",
						AName: "",
					},
					{
						Expr: &ast.FieldRef{
							StreamName: "table2",
							Name:       "c",
						},
						Name:  "c",
						AName: "",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		},
		{ // 3
			sql: `SELECT * FROM src1 INNER JOIN table1 ON src1.id = table1.id  WHERE table1.id > 10 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									LookupPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												WindowPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																baseLogicalPlan: baseLogicalPlan{},
																name:            "src1",
																streamStmt:      streams["src1"],
																streamFields:    map[string]*ast.JsonStreamField{},
																metaFields:      []string{},
																isWildCard:      true,
																isSchemaless:    true,
																pruneFields:     []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.TUMBLING_WINDOW,
													length:    10,
													timeUnit:  ast.SS,
													interval:  0,
													limit:     0,
												}.Init(),
											},
										},
										joinExpr: ast.Join{
											Name:     "table1",
											Alias:    "",
											JoinType: ast.INNER_JOIN,
											Expr: &ast.BinaryExpr{
												OP: ast.EQ,
												LHS: &ast.FieldRef{
													StreamName: "src1",
													Name:       "id",
												},
												RHS: &ast.FieldRef{
													StreamName: "table1",
													Name:       "id",
												},
											},
										},
										keys: []string{"id"},
										valvars: []ast.Expr{
											&ast.FieldRef{
												StreamName: "src1",
												Name:       "id",
											},
										},
										options: &ast.Options{
											DATASOURCE: "table1",
											TYPE:       "sql",
											KIND:       "lookup",
										},
										conditions: nil,
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								OP: ast.GT,
								LHS: &ast.FieldRef{
									StreamName: "table1",
									Name:       "id",
								},
								RHS: &ast.IntegerLiteral{Val: 10},
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
			}.Init(),
		},
		{ // 4
			sql: `SELECT * FROM src1 LEFT JOIN table1 ON src1.id = table1.id  WHERE table1.id > 10 GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									LookupPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												WindowPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																baseLogicalPlan: baseLogicalPlan{},
																name:            "src1",
																streamStmt:      streams["src1"],
																streamFields:    map[string]*ast.JsonStreamField{},
																metaFields:      []string{},
																isWildCard:      true,
																isSchemaless:    true,
																pruneFields:     []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.TUMBLING_WINDOW,
													length:    10,
													timeUnit:  ast.SS,
													interval:  0,
													limit:     0,
												}.Init(),
											},
										},
										joinExpr: ast.Join{
											Name:     "table1",
											Alias:    "",
											JoinType: ast.LEFT_JOIN,
											Expr: &ast.BinaryExpr{
												OP: ast.EQ,
												LHS: &ast.FieldRef{
													StreamName: "src1",
													Name:       "id",
												},
												RHS: &ast.FieldRef{
													StreamName: "table1",
													Name:       "id",
												},
											},
										},
										keys: []string{"id"},
										valvars: []ast.Expr{
											&ast.FieldRef{
												StreamName: "src1",
												Name:       "id",
											},
										},
										options: &ast.Options{
											DATASOURCE: "table1",
											TYPE:       "sql",
											KIND:       "lookup",
										},
										conditions: nil,
									}.Init(),
								},
							},
							condition: &ast.BinaryExpr{
								OP: ast.GT,
								LHS: &ast.FieldRef{
									StreamName: "table1",
									Name:       "id",
								},
								RHS: &ast.IntegerLiteral{Val: 10},
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
			}.Init(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
			assert.NoError(t, err)
			p, err := createLogicalPlan(stmt, &api.RuleOption{
				IsEventTime:        false,
				LateTol:            0,
				Concurrency:        0,
				BufferLength:       0,
				SendMetaToSink:     false,
				Qos:                0,
				CheckpointInterval: 0,
				SendError:          true,
			}, kv)
			assert.Equal(t, tt.err, testx.Errstring(err))
			if !assert.Equal(t, tt.p, p) {
				t.Errorf("stmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", render.AsCode(tt.p), render.AsCode(p))
			}
		})
	}
}

func TestTransformSourceNode(t *testing.T) {
	// add decompression for meta
	a1 := map[string]interface{}{
		"decompression": "gzip",
	}
	bs, err := json.Marshal(a1)
	assert.NoError(t, err)
	meta.InitYamlConfigManager()
	dataDir, _ := conf.GetDataLoc()
	err = os.MkdirAll(filepath.Join(dataDir, "sources"), 0o755)
	assert.NoError(t, err)
	err = meta.AddSourceConfKey("mqtt", "testCom", "", bs)
	assert.NoError(t, err)
	defer func() {
		err = meta.DelSourceConfKey("mqtt", "testCom", "")
		assert.NoError(t, err)
	}()
	// create expected nodes
	schema := map[string]*ast.JsonStreamField{
		"a": {
			Type: "bigint",
		},
	}
	props := nodeConf.GetSourceConf("mqtt", &ast.Options{TYPE: "mqtt"})
	srcNode, err := node.NewSourceConnectorNode("test", &mqtt.SourceConnector{}, "topic1", props, &api.RuleOption{SendError: false})
	assert.NoError(t, err)
	decodeNode, err := node.NewDecodeOp("2_decoder", "test", &api.RuleOption{SendError: false}, &ast.Options{TYPE: "mqtt"}, false, false, schema)
	assert.NoError(t, err)
	decomNode, err := node.NewDecompressOp("2_decompressor", &api.RuleOption{SendError: false}, "gzip")
	assert.NoError(t, err)
	decodeNode2, err := node.NewDecodeOp("3_decoder", "test", &api.RuleOption{SendError: false}, &ast.Options{TYPE: "mqtt"}, false, false, schema)
	assert.NoError(t, err)
	props2 := nodeConf.GetSourceConf("mqtt", &ast.Options{TYPE: "mqtt", CONF_KEY: "testCom"})
	srcNode2, err := node.NewSourceConnectorNode("test", &mqtt.SourceConnector{}, "topic1", props2, &api.RuleOption{SendError: false})
	assert.NoError(t, err)

	testCases := []struct {
		name string
		plan *DataSourcePlan
		node node.DataSourceNode
		ops  []node.OperatorNode
	}{
		{
			name: "normal source node",
			plan: &DataSourcePlan{
				name: "test",
				streamStmt: &ast.StreamStmt{
					StreamType: ast.TypeStream,
					Options: &ast.Options{
						TYPE: "file",
					},
				},
				streamFields: nil,
				allMeta:      false,
				metaFields:   []string{},
				iet:          false,
				isBinary:     false,
			},
			node: node.NewSourceNode("test", ast.TypeStream, nil, &ast.Options{
				TYPE: "file",
			}, &api.RuleOption{SendError: false}, false, false, nil),
		},
		{
			name: "schema source node",
			plan: &DataSourcePlan{
				name: "test",
				streamStmt: &ast.StreamStmt{
					StreamType: ast.TypeStream,
					Options: &ast.Options{
						TYPE: "file",
					},
				},
				streamFields: schema,
				allMeta:      false,
				metaFields:   []string{},
				iet:          false,
				isBinary:     false,
			},
			node: node.NewSourceNode("test", ast.TypeStream, nil, &ast.Options{
				TYPE: "file",
			}, &api.RuleOption{SendError: false}, false, false, schema),
		},
		{
			name: "split source node",
			plan: &DataSourcePlan{
				name: "test",
				streamStmt: &ast.StreamStmt{
					StreamType: ast.TypeStream,
					Options: &ast.Options{
						TYPE:       "mqtt",
						DATASOURCE: "topic1",
					},
				},
				streamFields: schema,
				allMeta:      false,
				metaFields:   []string{},
				iet:          false,
				isBinary:     false,
			},
			node: srcNode,
			ops: []node.OperatorNode{
				decodeNode,
			},
		},
		{
			name: "split source node with decompression",
			plan: &DataSourcePlan{
				name: "test",
				streamStmt: &ast.StreamStmt{
					StreamType: ast.TypeStream,
					Options: &ast.Options{
						TYPE:       "mqtt",
						DATASOURCE: "topic1",
						CONF_KEY:   "testCom",
					},
				},
				streamFields: schema,
				allMeta:      false,
				metaFields:   []string{},
				iet:          false,
				isBinary:     false,
			},
			node: srcNode2,
			ops: []node.OperatorNode{
				decomNode,
				decodeNode2,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sourceNode, ops, _, err := transformSourceNode(tc.plan, nil, "test", &api.RuleOption{}, 1)
			assert.NoError(t, err)
			assert.Equal(t, tc.node, sourceNode)
			assert.Equal(t, len(tc.ops), len(ops))
		})
	}
}

func TestGetLogicalPlanForExplain(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					myarray array(string)
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"src2": `CREATE STREAM src2 (
					id2 BIGINT,
					hum BIGINT
				) WITH (DATASOURCE="src2", FORMAT="json", KEY="ts", TIMESTAMP_FORMAT="YYYY-MM-dd HH:mm:ss");`,
		"tableInPlanner": `CREATE TABLE tableInPlanner (
					id BIGINT,
					name STRING,
					value STRING,
					hum BIGINT
				) WITH (TYPE="file");`,
	}
	types := map[string]ast.StreamType{
		"src1":           ast.TypeStream,
		"src2":           ast.TypeStream,
		"tableInPlanner": ast.TypeTable,
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

	ref := &ast.AliasRef{
		Expression: &ast.Call{
			Name:     "row_number",
			FuncType: ast.FuncTypeWindow,
		},
	}
	ref.SetRefSource([]string{})

	tests := []struct {
		rule *api.Rule
		res  string
		err  string
	}{
		{
			rule: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "select name, row_number() as index from src1",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			res: "{\"type\":\"ProjectPlan\",\"info\":\"Fields:[ $$alias.index, src1.name ]\",\"id\":0,\"children\":[1]}\n\n   {\"type\":\"WindowFuncPlan\",\"info\":\"windowFuncFields:[ {name:index, expr:$$alias.index} ]\",\"id\":1,\"children\":[2]}\n\n         {\"type\":\"DataSourcePlan\",\"info\":\"StreamName: src1, StreamFields:[ name ]\",\"id\":2,\"children\":null}\n\n",
		},
		{
			rule: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "select name, row_number() from src1",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			res: "{\"type\":\"ProjectPlan\",\"info\":\"Fields:[ src1.name, Call:{ name:row_number } ]\",\"id\":0,\"children\":[1]}\n\n   {\"type\":\"WindowFuncPlan\",\"info\":\"windowFuncFields:[ {name:row_number, expr:Call:{ name:row_number }} ]\",\"id\":1,\"children\":[2]}\n\n         {\"type\":\"DataSourcePlan\",\"info\":\"StreamName: src1, StreamFields:[ name ]\",\"id\":2,\"children\":null}\n\n",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		explain, err := GetExplainInfoFromLogicalPlan(tt.rule)
		if err != nil {
			t.Errorf(err.Error())
		}
		if !reflect.DeepEqual(explain, tt.res) {
			t.Errorf("case %d: expect validate %v but got %v", i, tt.res, explain)
		}
	}
}

func TestPlanTopo(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt");`,
		"src2": `CREATE STREAM src2 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt", SHARED="true");`,
		"src3": `CREATE STREAM src3 () WITH (DATASOURCE="topic1", FORMAT="json", TYPE="mqtt", CONF_KEY="testSel");`,
		"src4": `CREATE STREAM src4 () WITH (DATASOURCE="topic1", FORMAT="json", TYPE="mqtt", CONF_KEY="testSel",SHARED="true");`,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: ast.TypeStream,
			Statement:  sql,
		})
		assert.NoError(t, err)
		err = kv.Set(name, string(s))
		assert.NoError(t, err)
	}
	// add connectionSelector for meta
	a1 := map[string]interface{}{
		"connectionSelector": "mqtt.localConnection",
	}
	bs, err := json.Marshal(a1)
	assert.NoError(t, err)
	meta.InitYamlConfigManager()
	dataDir, _ := conf.GetDataLoc()
	err = os.MkdirAll(filepath.Join(dataDir, "sources"), 0o755)
	assert.NoError(t, err)
	err = meta.AddSourceConfKey("mqtt", "testSel", "", bs)
	assert.NoError(t, err)
	defer func() {
		err = meta.DelSourceConfKey("mqtt", "testSel", "")
		assert.NoError(t, err)
	}()
	tests := []struct {
		name string
		sql  string
		topo *api.PrintableTopo
	}{
		{
			name: "testMqttSplit",
			sql:  `SELECT * FROM src1`,
			topo: &api.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_2_decoder",
					},
					"op_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"sink_sink_memory_log",
					},
				},
			},
		},
		{
			name: "testSharedMqttSplit",
			sql:  `SELECT * FROM src2`,
			topo: &api.PrintableTopo{
				Sources: []string{"source_src2"},
				Edges: map[string][]any{
					"source_src2": {
						"op_src2_2_decoder",
					},
					"op_src2_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"sink_sink_memory_log",
					},
				},
			},
		},
		{
			name: "testSharedConnSplit",
			sql:  `SELECT * FROM src3`,
			topo: &api.PrintableTopo{
				Sources: []string{"source_mqtt.localConnection/topic1"},
				Edges: map[string][]any{
					"source_mqtt.localConnection/topic1": {
						"op_2_decoder",
					},
					"op_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"sink_sink_memory_log",
					},
				},
			},
		},
		{
			name: "testSharedNodeWithSharedConnSplit",
			sql:  `SELECT * FROM src4`,
			topo: &api.PrintableTopo{
				Sources: []string{"source_mqtt.localConnection/topic1"},
				Edges: map[string][]any{
					"source_mqtt.localConnection/topic1": {
						"op_src4_2_decoder",
					},
					"op_src4_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"sink_sink_memory_log",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topo, err := PlanSQLWithSourcesAndSinks(api.GetDefaultRule(tt.name, tt.sql), nil, []*node.SinkNode{node.NewSinkNode("sink_memory_log", "logToMemory", nil)})
			assert.NoError(t, err)
			assert.Equal(t, tt.topo, topo.GetTopo())
		})
	}
}
