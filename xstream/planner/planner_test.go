package planner

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"reflect"
	"strings"
	"testing"
)

func Test_createLogicalPlan(t *testing.T) {
	var tests = []struct {
		sql string
		p   LogicalPlan
		err string
	}{
		{ // 0
			sql: `SELECT name FROM tbl`,
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "tbl",
							isWildCard:      true,
							needMeta:        false,
							fields:          nil,
							metaFields:      nil,
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
			sql: `SELECT abc FROM src1 WHERE f1 = "v1" GROUP BY TUMBLINGWINDOW(ss, 10)`,
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
													name:       "src1",
													isWildCard: true,
													needMeta:   false,
													fields:     nil,
													metaFields: nil,
												}.Init(),
											},
										},
										condition: &xsql.BinaryExpr{
											LHS: &xsql.FieldRef{Name: "f1"},
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
						Expr:  &xsql.FieldRef{Name: "abc"},
						Name:  "abc",
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
						FilterPlan{
							baseLogicalPlan: baseLogicalPlan{
								children: []LogicalPlan{
									JoinPlan{
										baseLogicalPlan: baseLogicalPlan{
											children: []LogicalPlan{
												WindowPlan{
													baseLogicalPlan: baseLogicalPlan{
														children: []LogicalPlan{
															DataSourcePlan{
																name:       "src1",
																isWildCard: true,
																needMeta:   false,
																fields:     nil,
																metaFields: nil,
															}.Init(),
															DataSourcePlan{
																name:       "src2",
																isWildCard: true,
																needMeta:   false,
																fields:     nil,
																metaFields: nil,
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
												OP:  xsql.EQ,
												LHS: &xsql.FieldRef{Name: "id1", StreamName: "src1"},
												RHS: &xsql.FieldRef{Name: "id2", StreamName: "src2"},
											},
										}},
									}.Init(),
								},
							},
							condition: &xsql.BinaryExpr{
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
			sql: `SELECT abc FROM src1 WHERE f1 = "v1" GROUP BY TUMBLINGWINDOW(ss, 10) FILTER( WHERE size > 2)`,
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
													name:       "src1",
													isWildCard: true,
													needMeta:   false,
													fields:     nil,
													metaFields: nil,
												}.Init(),
											},
										},
										condition: &xsql.BinaryExpr{
											OP: xsql.AND,
											LHS: &xsql.BinaryExpr{
												LHS: &xsql.FieldRef{Name: "f1"},
												OP:  xsql.EQ,
												RHS: &xsql.StringLiteral{Val: "v1"},
											},
											RHS: &xsql.BinaryExpr{
												LHS: &xsql.FieldRef{Name: "size"},
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
						Expr:  &xsql.FieldRef{Name: "abc"},
						Name:  "abc",
						AName: ""},
				},
				isAggregate: false,
				sendMeta:    false,
			}.Init(),
		}, { // 4. do not optimize count window
			sql: `SELECT * FROM demo WHERE temperature > 20 GROUP BY COUNTWINDOW(5,1) HAVING COUNT(*) > 2`,
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
																name:       "demo",
																isWildCard: true,
																needMeta:   false,
																fields:     nil,
																metaFields: nil,
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
											LHS: &xsql.FieldRef{Name: "temperature"},
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
		},
	}
	//TODO optimize having, optimize on
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
		}
		p, err := createLogicalPlan(stmt, &api.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
		})
		if err != nil {
			t.Errorf("%d. %q\n\nerror:%v\n\n", i, tt.sql, err)
		}
		if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.p, p)
		}
	}
}
