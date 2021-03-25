package planner

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"path"
	"reflect"
	"strings"
	"testing"
)

var (
	DbDir = getDbDir()
)

func getDbDir() string {
	common.InitConf()
	dbDir, err := common.GetDataLoc()
	if err != nil {
		common.Log.Panic(err)
	}
	common.Log.Infof("db location is %s", dbDir)
	return dbDir
}

func Test_createLogicalPlan(t *testing.T) {
	store := kv.GetDefaultKVStore(path.Join(DbDir, "stream"))
	err := store.Open()
	if err != nil {
		t.Error(err)
		return
	}
	defer store.Close()
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"src2": `CREATE STREAM src2 (
					id2 BIGINT,
					hum BIGINT
				) WITH (DATASOURCE="src2", FORMAT="json", KEY="ts");`,
		"table1": `CREATE TABLE table1 (
					id BIGINT,
					name STRING,
					value STRING,
					hum BIGINT
				) WITH (TYPE="file");`,
	}
	types := map[string]xsql.StreamType{
		"src1":   xsql.TypeStream,
		"src2":   xsql.TypeStream,
		"table1": xsql.TypeTable,
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
		store.Set(name, string(s))
	}
	streams := make(map[string]*xsql.StreamStmt)
	for n, _ := range streamSqls {
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
			sql: `SELECT name FROM src1`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: []interface{}{
								&xsql.StreamField{
									Name:      "name",
									FieldType: &xsql.BasicType{Type: xsql.STRINGS},
								},
							},
							streamStmt: streams["src1"],
							metaFields: []string{},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
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
														&xsql.StreamField{
															Name:      "name",
															FieldType: &xsql.BasicType{Type: xsql.STRINGS},
														},
														&xsql.StreamField{
															Name:      "temp",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
												}.Init(),
											},
										},
										condition: &xsql.BinaryExpr{
											LHS: &xsql.FieldRef{Name: "name"},
											OP:  xsql.EQ,
											RHS: &xsql.StringLiteral{Val: "v1"},
										},
									}.Init(),
								},
							},
							condition: nil,
							wtype:     xsql.TUMBLING_WINDOW,
							length:    10000,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "temp"},
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
														&xsql.StreamField{
															Name:      "id1",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
														&xsql.StreamField{
															Name:      "temp",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: []interface{}{
														&xsql.StreamField{
															Name:      "hum",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
														&xsql.StreamField{
															Name:      "id2",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
													},
													streamStmt: streams["src2"],
													metaFields: []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     xsql.TUMBLING_WINDOW,
										length:    10000,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							from: &xsql.Table{Name: "src1"},
							joins: xsql.Joins{xsql.Join{
								Name:     "src2",
								JoinType: xsql.INNER_JOIN,
								Expr: &xsql.BinaryExpr{
									OP: xsql.AND,
									LHS: &xsql.BinaryExpr{
										LHS: &xsql.BinaryExpr{
											OP:  xsql.GT,
											LHS: &xsql.FieldRef{Name: "temp", StreamName: "src1"},
											RHS: &xsql.IntegerLiteral{Val: 20},
										},
										OP: xsql.OR,
										RHS: &xsql.BinaryExpr{
											OP:  xsql.GT,
											LHS: &xsql.FieldRef{Name: "hum", StreamName: "src2"},
											RHS: &xsql.IntegerLiteral{Val: 60},
										},
									},
									RHS: &xsql.BinaryExpr{
										OP:  xsql.EQ,
										LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
										RHS: &xsql.FieldRef{Name: "id2", StreamName: "src2"},
									},
								},
							}},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "id1"},
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
														&xsql.StreamField{
															Name:      "id1",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
														&xsql.StreamField{
															Name:      "name",
															FieldType: &xsql.BasicType{Type: xsql.STRINGS},
														},
														&xsql.StreamField{
															Name:      "temp",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
													},
													streamStmt: streams["src1"],
													metaFields: []string{},
												}.Init(),
											},
										},
										condition: &xsql.BinaryExpr{
											OP: xsql.AND,
											LHS: &xsql.BinaryExpr{
												LHS: &xsql.FieldRef{Name: "name"},
												OP:  xsql.EQ,
												RHS: &xsql.StringLiteral{Val: "v1"},
											},
											RHS: &xsql.BinaryExpr{
												LHS: &xsql.FieldRef{Name: "temp"},
												OP:  xsql.GT,
												RHS: &xsql.IntegerLiteral{Val: 2},
											},
										},
									}.Init(),
								},
							},
							condition: nil,
							wtype:     xsql.TUMBLING_WINDOW,
							length:    10000,
							interval:  0,
							limit:     0,
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "id1"},
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
																	&xsql.StreamField{
																		Name:      "id1",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "temp",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "name",
																		FieldType: &xsql.BasicType{Type: xsql.STRINGS},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     xsql.COUNT_WINDOW,
													length:    5,
													interval:  1,
													limit:     0,
												}.Init(),
											},
										},
										condition: &xsql.BinaryExpr{
											LHS: &xsql.FieldRef{Name: "temp"},
											OP:  xsql.GT,
											RHS: &xsql.IntegerLiteral{Val: 20},
										},
									}.Init(),
								},
							},
							condition: &xsql.BinaryExpr{
								LHS: &xsql.Call{Name: "COUNT", Args: []xsql.Expr{&xsql.StringLiteral{
									Val: "*",
								}}},
								OP:  xsql.GT,
								RHS: &xsql.IntegerLiteral{Val: 2},
							},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.Wildcard{Token: xsql.ASTERISK},
						Name:  "",
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
																	&xsql.StreamField{
																		Name:      "id1",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "temp",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: &xsql.BinaryExpr{
														RHS: &xsql.BinaryExpr{
															OP:  xsql.GT,
															LHS: &xsql.FieldRef{Name: "temp", StreamName: "src1"},
															RHS: &xsql.IntegerLiteral{Val: 20},
														},
														OP: xsql.AND,
														LHS: &xsql.BinaryExpr{
															OP:  xsql.GT,
															LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
															RHS: &xsql.IntegerLiteral{Val: 111},
														},
													},
												}.Init(),
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "src2",
																streamFields: []interface{}{
																	&xsql.StreamField{
																		Name:      "hum",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "id2",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																},
																streamStmt: streams["src2"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: &xsql.BinaryExpr{
														OP:  xsql.LT,
														LHS: &xsql.FieldRef{Name: "hum", StreamName: "src2"},
														RHS: &xsql.IntegerLiteral{Val: 60},
													},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     xsql.TUMBLING_WINDOW,
										length:    10000,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							from: &xsql.Table{
								Name: "src1",
							},
							joins: []xsql.Join{
								{
									Name:     "src2",
									Alias:    "",
									JoinType: xsql.INNER_JOIN,
									Expr: &xsql.BinaryExpr{
										LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
										OP:  xsql.EQ,
										RHS: &xsql.FieldRef{Name: "id2", StreamName: "src2"},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "id1"},
						Name:  "id1",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 6. optimize outter join on
			sql: `SELECT id1 FROM src1 FULL JOIN src2 on src1.id1 = src2.id2 and src1.temp > 20 and src2.hum < 60 WHERE src1.id > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
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
																	&xsql.StreamField{
																		Name:      "id1",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "temp",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: &xsql.BinaryExpr{
														OP:  xsql.GT,
														LHS: &xsql.FieldRef{Name: "id", StreamName: "src1"},
														RHS: &xsql.IntegerLiteral{Val: 111},
													},
												}.Init(),
												DataSourcePlan{
													name: "src2",
													streamFields: []interface{}{
														&xsql.StreamField{
															Name:      "hum",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
														&xsql.StreamField{
															Name:      "id2",
															FieldType: &xsql.BasicType{Type: xsql.BIGINT},
														},
													},
													streamStmt: streams["src2"],
													metaFields: []string{},
												}.Init(),
											},
										},
										condition: nil,
										wtype:     xsql.TUMBLING_WINDOW,
										length:    10000,
										interval:  0,
										limit:     0,
									}.Init(),
								},
							},
							from: &xsql.Table{
								Name: "src1",
							},
							joins: []xsql.Join{
								{
									Name:     "src2",
									Alias:    "",
									JoinType: xsql.FULL_JOIN,
									Expr: &xsql.BinaryExpr{
										OP: xsql.AND,
										LHS: &xsql.BinaryExpr{
											OP: xsql.AND,
											LHS: &xsql.BinaryExpr{
												LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
												OP:  xsql.EQ,
												RHS: &xsql.FieldRef{Name: "id2", StreamName: "src2"},
											},
											RHS: &xsql.BinaryExpr{
												OP:  xsql.GT,
												LHS: &xsql.FieldRef{Name: "temp", StreamName: "src1"},
												RHS: &xsql.IntegerLiteral{Val: 20},
											},
										},
										RHS: &xsql.BinaryExpr{
											OP:  xsql.LT,
											LHS: &xsql.FieldRef{Name: "hum", StreamName: "src2"},
											RHS: &xsql.IntegerLiteral{Val: 60},
										},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "id1"},
						Name:  "id1",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 7 window error for table
			sql: `SELECT value FROM table1 WHERE name = "v1" GROUP BY TUMBLINGWINDOW(ss, 10) FILTER( WHERE temp > 2)`,
			p:   nil,
			err: "cannot run window for TABLE sources",
		}, { // 8 join table without window
			sql: `SELECT id1 FROM src1 INNER JOIN table1 on src1.id1 = table1.id and src1.temp > 20 and table1.hum < 60 WHERE src1.id1 > 111`,
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
																	&xsql.StreamField{
																		Name:      "id1",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "temp",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																},
																streamStmt: streams["src1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: &xsql.BinaryExpr{
														RHS: &xsql.BinaryExpr{
															OP:  xsql.GT,
															LHS: &xsql.FieldRef{Name: "temp", StreamName: "src1"},
															RHS: &xsql.IntegerLiteral{Val: 20},
														},
														OP: xsql.AND,
														LHS: &xsql.BinaryExpr{
															OP:  xsql.GT,
															LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
															RHS: &xsql.IntegerLiteral{Val: 111},
														},
													},
												}.Init(),
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "table1",
																streamFields: []interface{}{
																	&xsql.StreamField{
																		Name:      "hum",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "id",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																},
																streamStmt: streams["table1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: &xsql.BinaryExpr{
														OP:  xsql.LT,
														LHS: &xsql.FieldRef{Name: "hum", StreamName: "table1"},
														RHS: &xsql.IntegerLiteral{Val: 60},
													},
												}.Init(),
											},
										},
										Emitters: []string{"table1"},
									}.Init(),
								},
							},
							from: &xsql.Table{
								Name: "src1",
							},
							joins: []xsql.Join{
								{
									Name:     "table1",
									Alias:    "",
									JoinType: xsql.INNER_JOIN,
									Expr: &xsql.BinaryExpr{
										LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
										OP:  xsql.EQ,
										RHS: &xsql.FieldRef{Name: "id", StreamName: "table1"},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "id1"},
						Name:  "id1",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 8 join table with window
			sql: `SELECT id1 FROM src1 INNER JOIN table1 on src1.id1 = table1.id and src1.temp > 20 and table1.hum < 60 WHERE src1.id1 > 111 GROUP BY TUMBLINGWINDOW(ss, 10)`,
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
															FilterPlan{
																baseLogicalPlan: baseLogicalPlan{
																	children: []LogicalPlan{
																		DataSourcePlan{
																			name: "src1",
																			streamFields: []interface{}{
																				&xsql.StreamField{
																					Name:      "id1",
																					FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																				},
																				&xsql.StreamField{
																					Name:      "temp",
																					FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																				},
																			},
																			streamStmt: streams["src1"],
																			metaFields: []string{},
																		}.Init(),
																	},
																},
																condition: &xsql.BinaryExpr{
																	RHS: &xsql.BinaryExpr{
																		OP:  xsql.GT,
																		LHS: &xsql.FieldRef{Name: "temp", StreamName: "src1"},
																		RHS: &xsql.IntegerLiteral{Val: 20},
																	},
																	OP: xsql.AND,
																	LHS: &xsql.BinaryExpr{
																		OP:  xsql.GT,
																		LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
																		RHS: &xsql.IntegerLiteral{Val: 111},
																	},
																},
															}.Init(),
														},
													},
													condition: nil,
													wtype:     xsql.TUMBLING_WINDOW,
													length:    10000,
													interval:  0,
													limit:     0,
												}.Init(),
												FilterPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name: "table1",
																streamFields: []interface{}{
																	&xsql.StreamField{
																		Name:      "hum",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																	&xsql.StreamField{
																		Name:      "id",
																		FieldType: &xsql.BasicType{Type: xsql.BIGINT},
																	},
																},
																streamStmt: streams["table1"],
																metaFields: []string{},
															}.Init(),
														},
													},
													condition: &xsql.BinaryExpr{
														OP:  xsql.LT,
														LHS: &xsql.FieldRef{Name: "hum", StreamName: "table1"},
														RHS: &xsql.IntegerLiteral{Val: 60},
													},
												}.Init(),
											},
										},
										Emitters: []string{"table1"},
									}.Init(),
								},
							},
							from: &xsql.Table{
								Name: "src1",
							},
							joins: []xsql.Join{
								{
									Name:     "table1",
									Alias:    "",
									JoinType: xsql.INNER_JOIN,
									Expr: &xsql.BinaryExpr{
										LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
										OP:  xsql.EQ,
										RHS: &xsql.FieldRef{Name: "id", StreamName: "table1"},
									},
								},
							},
						}.Init(),
					},
				},
				fields: []xsql.Field{
					{
						Expr:  &xsql.FieldRef{Name: "id1"},
						Name:  "id1",
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
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		} else if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.p, p)
		}
	}
}

// errstring returns the string representation of an error.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
