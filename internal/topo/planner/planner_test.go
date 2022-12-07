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
	"encoding/json"
	"fmt"
	"github.com/gdexlab/go-render/render"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"strings"
	"testing"
)

func init() {
	testx.InitEnv()
}

func Test_createLogicalPlan(t *testing.T) {
	err, store := store.GetKV("stream")
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
		err = store.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	var (
		//boolTrue = true
		boolFalse = false
	)

	var tests = []struct {
		sql string
		p   LogicalPlan
		err string
	}{
		{ // 0
			sql: `SELECT myarray[temp] FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: []interface{}{
								&ast.StreamField{
									Name:      "myarray",
									FieldType: &ast.ArrayType{Type: ast.STRINGS},
								},
								&ast.StreamField{
									Name:      "temp",
									FieldType: &ast.BasicType{Type: ast.BIGINT},
								},
							},
							streamStmt: streams["src1"],
							metaFields: []string{},
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
		}, { // 1 optimize where to data source
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "name",
															FieldType: &ast.BasicType{Type: ast.STRINGS},
														},
														&ast.StreamField{
															Name:      "temp",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
							length:    10000,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 2 condition that cannot be optimized
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "id1",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "temp",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id2",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 3 optimize window filter
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "id1",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "name",
															FieldType: &ast.BasicType{Type: ast.STRINGS},
														},
														&ast.StreamField{
															Name:      "temp",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
							length:    10000,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 4. do not optimize count window
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "id1",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "temp",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "name",
																		FieldType: &ast.BasicType{Type: ast.STRINGS},
																	},
																	&ast.StreamField{
																		Name:      "myarray",
																		FieldType: &ast.ArrayType{Type: ast.STRINGS},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 5. optimize join on
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "id1",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "temp",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "hum",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "id2",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																},
																streamStmt:      streams["src2"],
																metaFields:      []string{},
																timestampFormat: "YYYY-MM-dd HH:mm:ss",
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
										length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 6. optimize outter join on
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "id1",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "temp",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id2",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 7 window error for table
			sql: `SELECT value FROM tableInPlanner WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p:   nil,
			err: "cannot run window for TABLE sources",
		}, { // 8 join table without window
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "id1",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "temp",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["tableInPlanner"],
													metaFields: []string{},
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 9 join table with window
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "id1",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "temp",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.TUMBLING_WINDOW,
													length:    10000,
													interval:  0,
													limit:     0,
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["tableInPlanner"],
													metaFields: []string{},
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 10 meta
			sql: `SELECT temp, meta(id) AS eid,meta(Humidity->Device) AS hdevice FROM src1 WHERE meta(device)="demo2"`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: []interface{}{
											&ast.StreamField{
												Name:      "temp",
												FieldType: &ast.BasicType{Type: ast.BIGINT},
											},
										},
										streamStmt: streams["src1"],
										metaFields: []string{"Humidity", "device", "id"},
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
		}, { // 11 join with same name field and aliased
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id2",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt:      streams["src2"],
													metaFields:      []string{},
													timestampFormat: "YYYY-MM-dd HH:mm:ss",
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["tableInPlanner"],
													metaFields: []string{},
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
		}, { // 12 meta with more fields
			sql: `SELECT temp, meta(*) as m FROM src1 WHERE meta(device)="demo2"`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: []interface{}{
											&ast.StreamField{
												Name:      "temp",
												FieldType: &ast.BasicType{Type: ast.BIGINT},
											},
										},
										streamStmt: streams["src1"],
										metaFields: []string{},
										allMeta:    true,
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
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: "",
					}, {
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
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 13 analytic function plan
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "id1",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "name",
															FieldType: &ast.BasicType{Type: ast.STRINGS},
														},
														&ast.StreamField{
															Name:      "temp",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
												Name: "latest", FuncId: 1, CachedField: "$$a_latest_1", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}},
											},
											{
												Name: "lag", FuncId: 0, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}},
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
							streamFields: []interface{}{
								&ast.StreamField{
									Name:      "id1",
									FieldType: &ast.BasicType{Type: ast.BIGINT},
								},
								&ast.StreamField{
									Name:      "temp",
									FieldType: &ast.BasicType{Type: ast.BIGINT},
								},
								&ast.StreamField{
									Name:      "name",
									FieldType: &ast.BasicType{Type: ast.STRINGS},
								},
								&ast.StreamField{
									Name:      "myarray",
									FieldType: &ast.ArrayType{Type: ast.STRINGS},
								},
							},
							streamStmt: streams["src1"],
							metaFields: []string{"device"},
							isWildCard: true,
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
			sql: `SELECT latest(lag(name)) OVER (PARTITION BY temp), id1 FROM src1 WHERE lag(temp) > temp`,
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "id1",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "name",
															FieldType: &ast.BasicType{Type: ast.STRINGS},
														},
														&ast.StreamField{
															Name:      "temp",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
												Name: "latest", FuncId: 1, CachedField: "$$a_latest_1", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}}, Partition: &ast.PartitionExpr{Exprs: []ast.Expr{&ast.FieldRef{Name: "temp", StreamName: "src1"}}},
											},
											{
												Name: "lag", FuncId: 0, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}},
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
			sql: `SELECT latest(lag(name)) OVER (PARTITION BY temp WHEN temp > 12), id1 FROM src1 WHERE lag(temp) > temp`,
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "id1",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "name",
															FieldType: &ast.BasicType{Type: ast.STRINGS},
														},
														&ast.StreamField{
															Name:      "temp",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
												Name: "latest", FuncId: 1, CachedField: "$$a_latest_1", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.Call{Name: "lag", FuncId: 0, Cached: true, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}}}}, Partition: &ast.PartitionExpr{Exprs: []ast.Expr{&ast.FieldRef{Name: "temp", StreamName: "src1"}}}, WhenExpr: &ast.BinaryExpr{LHS: &ast.FieldRef{Name: "temp", StreamName: "src1"}, OP: ast.GT, RHS: &ast.IntegerLiteral{Val: 12}},
											},
											{
												Name: "lag", FuncId: 0, CachedField: "$$a_lag_0", FuncType: ast.FuncTypeScalar, Args: []ast.Expr{&ast.FieldRef{Name: "name", StreamName: "src1"}},
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
						Expr: &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name: "id1",
					},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 17. do not optimize sliding window
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
																streamFields: []interface{}{
																	&ast.StreamField{
																		Name:      "id1",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "temp",
																		FieldType: &ast.BasicType{Type: ast.BIGINT},
																	},
																	&ast.StreamField{
																		Name:      "name",
																		FieldType: &ast.BasicType{Type: ast.STRINGS},
																	},
																	&ast.StreamField{
																		Name:      "myarray",
																		FieldType: &ast.ArrayType{Type: ast.STRINGS},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.SLIDING_WINDOW,
													length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
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
		}, store)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		} else if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, render.AsCode(tt.p), render.AsCode(p))
		}
	}
}

func Test_createLogicalPlanSchemaless(t *testing.T) {
	err, store := store.GetKV("stream")
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
		err = store.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	var (
		//boolTrue = true
		boolFalse = false
	)

	var tests = []struct {
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
							streamFields: []interface{}{
								"name",
							},
							streamStmt: streams["src1"],
							metaFields: []string{},
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
		}, { // 1 optimize where to data source
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
													streamFields: []interface{}{
														"name", "temp",
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
							length:    10000,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "temp", StreamName: "src1"},
						Name:  "temp",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 2 condition that cannot be optimized
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
													streamFields: []interface{}{
														"id1", "temp",
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: []interface{}{ // can't determine where is id1 belonged to
														"hum", "id1", "id2",
													},
													streamStmt: streams["src2"],
													metaFields: []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 3 optimize window filter
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
													streamFields: []interface{}{
														"id1", "name", "temp",
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
							length:    10000,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						Expr:  &ast.FieldRef{Name: "id1", StreamName: "src1"},
						Name:  "id1",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 4. do not optimize count window
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
																streamFields: nil,
																streamStmt:   streams["src1"],
																metaFields:   []string{},
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 5. optimize join on
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
																streamFields: []interface{}{
																	"id1", "temp",
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
																streamFields: []interface{}{
																	"hum", "id1", "id2",
																},
																streamStmt: streams["src2"],
																metaFields: []string{},
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
										length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 6. optimize outter join on
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
																streamFields: []interface{}{
																	"id1", "temp",
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
													streamFields: []interface{}{
														"hum", "id1", "id2",
													},
													streamStmt: streams["src2"],
													metaFields: []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10000,
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 7 window error for table
			sql: `SELECT value FROM tableInPlanner WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p:   nil,
			err: "cannot run window for TABLE sources",
		}, { // 8 join table without window
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
																streamFields: []interface{}{
																	"hum", "id1", "temp",
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["tableInPlanner"],
													metaFields: []string{},
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 9 join table with window
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
																streamFields: []interface{}{
																	"id1", "temp",
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     ast.TUMBLING_WINDOW,
													length:    10000,
													interval:  0,
													limit:     0,
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["tableInPlanner"],
													metaFields: []string{},
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
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 10 meta
			sql: `SELECT temp, meta(id) AS eid,meta(Humidity->Device) AS hdevice FROM src1 WHERE meta(device)="demo2"`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									DataSourcePlan{
										name: "src1",
										streamFields: []interface{}{
											"temp",
										},
										streamStmt: streams["src1"],
										metaFields: []string{"Humidity", "device", "id"},
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
		}, { // 11 join with same name field and aliased
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
													streamFields: []interface{}{
														"hum", "id", "id2",
													},
													streamStmt: streams["src2"],
													metaFields: []string{},
												}.Init(),
												DataSourcePlan{
													name: "tableInPlanner",
													streamFields: []interface{}{
														&ast.StreamField{
															Name:      "hum",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
														&ast.StreamField{
															Name:      "id",
															FieldType: &ast.BasicType{Type: ast.BIGINT},
														},
													},
													streamStmt: streams["tableInPlanner"],
													metaFields: []string{},
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
		}, { // 12
			sql: `SELECT name->first, name->last FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: []interface{}{
								"name",
							},
							streamStmt: streams["src1"],
							metaFields: []string{},
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
		}, store)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		} else if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, render.AsCode(tt.p), render.AsCode(p))
		}
	}
}

func Test_createLogicalPlan4Lookup(t *testing.T) {
	err, store := store.GetKV("stream")
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
		err = store.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}
	var tests = []struct {
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
										streamFields: []interface{}{
											"a",
										},
										streamStmt: streams["src1"],
										metaFields: []string{},
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
								DATASOURCE:        "table1",
								TYPE:              "sql",
								STRICT_VALIDATION: true,
								KIND:              "lookup",
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
																streamFields: []interface{}{
																	"a",
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
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
											DATASOURCE:        "table1",
											TYPE:              "sql",
											STRICT_VALIDATION: true,
											KIND:              "lookup",
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
													streamFields: []interface{}{
														"a",
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
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
											DATASOURCE:        "table1",
											TYPE:              "sql",
											STRICT_VALIDATION: true,
											KIND:              "lookup",
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
								DATASOURCE:        "table2",
								TYPE:              "sql",
								STRICT_VALIDATION: true,
								KIND:              "lookup",
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
			sql: `SELECT * FROM src1 INNER JOIN table1 ON src1.id = table1.id GROUP BY TUMBLINGWINDOW(ss, 10)`,
			p: ProjectPlan{
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
													metaFields:      []string{},
													isWildCard:      true,
												}.Init(),
											},
										},
										condition: nil,
										wtype:     ast.TUMBLING_WINDOW,
										length:    10000,
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
								DATASOURCE:        "table1",
								TYPE:              "sql",
								STRICT_VALIDATION: true,
								KIND:              "lookup",
							},
							conditions: nil,
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
		}, store)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		} else if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, render.AsCode(tt.p), render.AsCode(p))
		}
	}
}
