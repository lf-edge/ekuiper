package xsql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Ensure the parser can parse strings into Statement ASTs.
func TestParser_ParseStatement(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *SelectStatement
		err  string
	}{
		{
			s: `SELECT name FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},
		{
			s: "SELECT `select` FROM tbl",
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "select"},
						Name:  "select",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},
		{
			s: `SELECT name FROM topic/sensor1`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1"}},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
			},
		},
		{
			s: "SELECT t1.name FROM topic/sensor1 AS `join`",
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "join"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1 AS t1`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1/#`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1/#"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1/# AS t2 `,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1/#", Alias: "t2"}},
			},
		},

		{
			s: `SELECT name FROM /topic/sensor1/#`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "/topic/sensor1/#"}},
			},
		},

		{
			s: `SELECT name FROM /topic/sensor1/#/`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "/topic/sensor1/#/"}},
			},
		},

		{
			s: `SELECT name FROM /topic/sensor1/+/temp1/`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "/topic/sensor1/+/temp1/"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1/+/temp`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1/+/temp"}},
			},
		},

		{
			s: `SELECT * FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},
		{
			s: `SELECT a,b FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "a"}, Name: "a", AName: ""},
					{Expr: &FieldRef{Name: "b"}, Name: "b", AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},
		{
			s: `SELECT a, b,c FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "a"}, Name: "a", AName: ""},
					{Expr: &FieldRef{Name: "b"}, Name: "b", AName: ""},
					{Expr: &FieldRef{Name: "c"}, Name: "c", AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT a AS alias FROM tbl`,
			stmt: &SelectStatement{
				Fields:  []Field{{Expr: &FieldRef{Name: "a"}, Name: "a", AName: "alias"}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT a AS alias1, b as Alias2 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "a"}, Name: "a", AName: "alias1"},
					{Expr: &FieldRef{Name: "b"}, Name: "b", AName: "Alias2"},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT length("test") FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "length",
						Expr: &Call{
							Name: "length",
							Args: []Expr{&StringLiteral{Val: "test"}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT length(test) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "length",
						Expr: &Call{
							Name: "length",
							Args: []Expr{&FieldRef{Name: "test"}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT sin(123) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "sin",
						Expr: &Call{
							Name: "sin",
							Args: []Expr{&IntegerLiteral{Val: 123}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT lpad("abc", 123) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "lpad",
						Expr: &Call{
							Name: "lpad",
							Args: []Expr{&StringLiteral{Val: "abc"}, &IntegerLiteral{Val: 123}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT newuuid() FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "newuuid",
						Expr: &Call{
							Name: "newuuid",
							Args: nil,
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT indexof("abc", field1) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "indexof",
						Expr: &Call{
							Name: "indexof",
							Args: []Expr{
								&StringLiteral{Val: "abc"},
								&FieldRef{Name: "field1"},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT lpad(lower(test),1) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "lpad",
						Expr: &Call{
							Name: "lpad",
							Args: []Expr{
								&Call{
									Name: "lower",
									Args: []Expr{
										&FieldRef{Name: "test"},
									},
								},
								&IntegerLiteral{Val: 1},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT lpad(lower(test),1) AS field1 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "field1",
						Name:  "lpad",
						Expr: &Call{
							Name: "lpad",
							Args: []Expr{
								&Call{
									Name: "lower",
									Args: []Expr{
										&FieldRef{Name: "test"},
									},
								},
								&IntegerLiteral{Val: 1},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT length(lower("test")) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "length",
						Expr: &Call{
							Name: "length",
							Args: []Expr{
								&Call{
									Name: "lower",
									Args: []Expr{
										&StringLiteral{Val: "test"},
									},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT count(*) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "count",
						Expr: &Call{
							Name: "count",
							Args: []Expr{&Wildcard{Token: ASTERISK}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT count(*, f1) FROM tbl`,
			stmt: nil,
			err:  `found ",", expected right paren.`,
		},

		{
			s: `SELECT deduplicate(temperature, false) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "deduplicate",
						Expr: &Call{
							Name: "deduplicate",
							Args: []Expr{&Wildcard{Token: ASTERISK}, &FieldRef{Name: "temperature"}, &BooleanLiteral{Val: false}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT "abc" FROM tbl`,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "", Expr: &StringLiteral{Val: "abc"}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT "abc" AS field1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "field1", Name: "", Expr: &StringLiteral{Val: "abc"}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT field0,   "abc" AS field1, field2 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{AName: "", Name: "field0", Expr: &FieldRef{Name: "field0"}},
					{AName: "field1", Name: "", Expr: &StringLiteral{Val: "abc"}},
					{AName: "", Name: "field2", Expr: &FieldRef{Name: "field2"}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT * AS alias FROM tbl`,
			stmt: nil,
			err:  `found "AS", expected FROM.`,
		},

		{
			s:    `SELECT *, FROM tbl`,
			stmt: nil,
			err:  `found ",", expected FROM.`,
		},

		{
			s:    `SELECTname FROM tbl`,
			stmt: nil,
			err:  `Found "SELECTname", Expected SELECT.` + "\n",
		},

		{
			s: `SELECT abc FROM tbl WHERE abc > 12 `,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "abc", Expr: &FieldRef{Name: "abc"}}},
				Sources: []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name: "abc"},
					OP:  GT,
					RHS: &IntegerLiteral{Val: 12},
				},
			},
		},

		{
			s: `SELECT abc FROM tbl WHERE abc = "hello" `,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "abc", Expr: &FieldRef{Name: "abc"}}},
				Sources: []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name: "abc"},
					OP:  EQ,
					RHS: &StringLiteral{Val: "hello"},
				},
			},
		},

		{
			s: `SELECT t1.abc FROM tbl AS t1 WHERE t1.abc = "hello" `,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "abc", Expr: &FieldRef{StreamName: StreamName("t1"), Name: "abc"}}},
				Sources: []Source{&Table{Name: "tbl", Alias: "t1"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{StreamName: StreamName("t1"), Name: "abc"},
					OP:  EQ,
					RHS: &StringLiteral{Val: "hello"},
				},
			},
		},

		{
			s: `SELECT abc, "fff" AS fa FROM tbl WHERE fa >= 5 `,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "abc", Expr: &FieldRef{Name: "abc"}}, {AName: "fa", Name: "", Expr: &StringLiteral{Val: "fff"}}},
				Sources: []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name: "fa"},
					OP:  GTE,
					RHS: &IntegerLiteral{Val: 5},
				},
			},
		},

		{
			s: `SELECT field2 FROM tbl WHERE field2 != 5 `,
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "field2", Expr: &FieldRef{Name: "field2"}}},
				Sources: []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name: "field2"},
					OP:  NEQ,
					RHS: &IntegerLiteral{Val: 5},
				},
			},
		},

		{
			s: `SELECT field2 FROM tbl WHERE field2 !   = 5 `, //Add space char in expression
			stmt: &SelectStatement{
				Fields:  []Field{{AName: "", Name: "field2", Expr: &FieldRef{Name: "field2"}}},
				Sources: []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name: "field2"},
					OP:  NEQ,
					RHS: &IntegerLiteral{Val: 5},
				},
			},
		},

		{
			s:    `SELECT *f FROM tbl`,
			stmt: nil,
			err:  `found "f", expected FROM.`,
		},

		////TODO
		//{
		//	s: `SELECT *from FROM tbl`,
		//	stmt: nil,
		//	err: `found "f", expected FROM.`,
		//},

		{
			s: `SELECT abc+2 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "",
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "abc"},
							OP:  ADD,
							RHS: &IntegerLiteral{Val: 2},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT t1.abc+2 FROM tbl AS t1`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "",
						Expr: &BinaryExpr{
							LHS: &FieldRef{StreamName: StreamName("t1"), Name: "abc"},
							OP:  ADD,
							RHS: &IntegerLiteral{Val: 2},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl", Alias: "t1"}},
			},
		},

		{
			s: `SELECT abc + "hello" FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "",
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "abc"},
							OP:  ADD,
							RHS: &StringLiteral{Val: "hello"},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT abc*2 + 3 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "",
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{
								LHS: &FieldRef{Name: "abc"},
								OP:  MUL,
								RHS: &IntegerLiteral{Val: 2},
							},
							OP:  ADD,
							RHS: &IntegerLiteral{Val: 3},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT ln(abc*2 + 3) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "ln",
						Expr: &Call{
							Name: "ln",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{
										LHS: &FieldRef{Name: "abc"},
										OP:  MUL,
										RHS: &IntegerLiteral{Val: 2},
									},
									OP:  ADD,
									RHS: &IntegerLiteral{Val: 3},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT ln(t1.abc*2 + 3) FROM tbl AS t1`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "ln",
						Expr: &Call{
							Name: "ln",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{
										LHS: &FieldRef{StreamName: StreamName("t1"), Name: "abc"},
										OP:  MUL,
										RHS: &IntegerLiteral{Val: 2},
									},
									OP:  ADD,
									RHS: &IntegerLiteral{Val: 3},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl", Alias: "t1"}},
			},
		},

		{
			s: `SELECT lpad("param2", abc*2 + 3) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "lpad",
						Expr: &Call{
							Name: "lpad",
							Args: []Expr{
								&StringLiteral{Val: "param2"},
								&BinaryExpr{
									LHS: &BinaryExpr{
										LHS: &FieldRef{Name: "abc"},
										OP:  MUL,
										RHS: &IntegerLiteral{Val: 2},
									},
									OP:  ADD,
									RHS: &IntegerLiteral{Val: 3},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT 0.2 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "",
						Expr:  &NumberLiteral{Val: 0.2},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT .2 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "",
						Expr:  &NumberLiteral{Val: 0.2},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT sin(.2) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "sin",
						Expr: &Call{
							Name: "sin",
							Args: []Expr{&NumberLiteral{Val: 0.2}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT power(.2, 4) FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "power",
						Expr: &Call{
							Name: "power",
							Args: []Expr{&NumberLiteral{Val: 0.2}, &IntegerLiteral{Val: 4}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT power(.2, 4) AS f1 FROM tbl WHERE f1 > 2.2`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "f1",
						Name:  "power",
						Expr: &Call{
							Name: "power",
							Args: []Expr{&NumberLiteral{Val: 0.2}, &IntegerLiteral{Val: 4}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name: "f1"},
					OP:  GT,
					RHS: &NumberLiteral{Val: 2.2},
				},
			},
		},

		{
			s: `SELECT deviceId, name FROM topic/sensor1 WHERE deviceId=1 AND name = "dname"`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "deviceId"}, Name: "deviceId", AName: ""},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{LHS: &FieldRef{Name: "deviceId"}, OP: EQ, RHS: &IntegerLiteral{Val: 1}},
					OP:  AND,
					RHS: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				},
			},
		},

		{
			s: `SELECT deviceId, name FROM topic/sensor1 AS t1 WHERE t1.deviceId=1 AND t1.name = "dname"`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "deviceId"}, Name: "deviceId", AName: ""},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{LHS: &FieldRef{StreamName: StreamName("t1"), Name: "deviceId"}, OP: EQ, RHS: &IntegerLiteral{Val: 1}},
					OP:  AND,
					RHS: &BinaryExpr{LHS: &FieldRef{StreamName: StreamName("t1"), Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE t> = 20.5 OR name = "dname"`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{LHS: &FieldRef{Name: "t"}, OP: GTE, RHS: &NumberLiteral{Val: 20.5}},
					OP:  OR,
					RHS: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY name`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:    []Source{&Table{Name: "topic/sensor1"}},
				Condition:  &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{Dimension{Expr: &FieldRef{Name: "name"}}},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY name HAVING count(name) > 3`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:    []Source{&Table{Name: "topic/sensor1"}},
				Condition:  &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{Dimension{Expr: &FieldRef{Name: "name"}}},
				Having:     &BinaryExpr{LHS: &Call{Name: "count", Args: []Expr{&FieldRef{StreamName: "", Name: "name"}}}, OP: GT, RHS: &IntegerLiteral{Val: 3}},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" HAVING count(name) > 3`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:   []Source{&Table{Name: "topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Having:    &BinaryExpr{LHS: &Call{Name: "count", Args: []Expr{&FieldRef{StreamName: "", Name: "name"}}}, OP: GT, RHS: &IntegerLiteral{Val: 3}},
			},
		},

		{
			s:    `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" HAVING sin(name) > 0.3`,
			stmt: nil,
			err:  "Not allowed to call none-aggregate functions in HAVING clause.",
		},

		{
			s:    `SELECT id,AVG(data) FROM t GROUP BY SUM(data)>10`,
			stmt: nil,
			err:  "Not allowed to call aggregate functions in GROUP BY clause.",
		},

		{
			s:    `SELECT temp AS t, name FROM topic/sensor1 WHERE count(name) = 3`,
			stmt: nil,
			err:  "Not allowed to call aggregate functions in WHERE clause.",
		},

		{
			s: `SELECT s1.temp AS t, name FROM topic/sensor1 AS s1 WHERE t = "dname" GROUP BY s1.temp`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{StreamName: "s1", Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:    []Source{&Table{Name: "topic/sensor1", Alias: "s1"}},
				Condition:  &BinaryExpr{LHS: &FieldRef{Name: "t"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{Dimension{Expr: &FieldRef{StreamName: "s1", Name: "temp"}}},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY lpad(name,1)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:   []Source{&Table{Name: "topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{Dimension{
					Expr: &Call{Name: "lpad", Args: []Expr{&FieldRef{Name: "name"}, &IntegerLiteral{Val: 1}}},
				},
				},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 AS s1 WHERE name = "dname" GROUP BY lpad(s1.name,1)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:   []Source{&Table{Name: "topic/sensor1", Alias: "s1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{Dimension{
					Expr: &Call{Name: "lpad", Args: []Expr{&FieldRef{StreamName: StreamName("s1"), Name: "name"}, &IntegerLiteral{Val: 1}}},
				},
				},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY lpad(name,1) ORDER BY name`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:   []Source{&Table{Name: "topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Call{Name: "lpad", Args: []Expr{
							&FieldRef{Name: "name"},
							&IntegerLiteral{Val: 1}},
						},
					},
				},
				SortFields: []SortField{{Name: "name", Ascending: true}},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 AS s1 WHERE s1.name = "dname" GROUP BY lpad(s1.name,1) ORDER BY s1.name`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:   []Source{&Table{Name: "topic/sensor1", Alias: "s1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{StreamName: StreamName("s1"), Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Call{Name: "lpad", Args: []Expr{
							&FieldRef{StreamName: StreamName("s1"), Name: "name"},
							&IntegerLiteral{Val: 1}},
						},
					},
				},
				SortFields: []SortField{{Name: "s1\007name", Ascending: true}},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY lpad(name,1) ORDER BY name DESC`,
			stmt: &SelectStatement{
				Fields: []Field{
					{Expr: &FieldRef{Name: "temp"}, Name: "temp", AName: "t"},
					{Expr: &FieldRef{Name: "name"}, Name: "name", AName: ""},
				},
				Sources:   []Source{&Table{Name: "topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Call{Name: "lpad", Args: []Expr{
							&FieldRef{Name: "name"},
							&IntegerLiteral{Val: 1}},
						},
					},
				},
				SortFields: []SortField{{Name: "name", Ascending: false}},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 ORDER BY name DESC`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources:    []Source{&Table{Name: "topic/sensor1"}},
				SortFields: []SortField{{Name: "name", Ascending: false}},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 ORDER BY name DESC, name2 ASC`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources:    []Source{&Table{Name: "topic/sensor1"}},
				SortFields: []SortField{{Name: "name", Ascending: false}, {Name: "name2", Ascending: true}},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 GROUP BY name, name2,power(name3,1.8) ORDER BY name DESC, name2 ASC`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1"}},
				Dimensions: Dimensions{
					Dimension{Expr: &FieldRef{Name: "name"}},
					Dimension{Expr: &FieldRef{Name: "name2"}},
					Dimension{
						Expr: &Call{Name: "power", Args: []Expr{
							&FieldRef{Name: "name3"},
							&NumberLiteral{Val: 1.8}},
						},
					},
				},
				SortFields: []SortField{{Name: "name", Ascending: false}, {Name: "name2", Ascending: true}},
			},
		},

		//{
		//	s: `SELECT .2sd FROM tbl`,
		//	stmt: &SelectStatement{
		//		Fields:    []Field{
		//			Field{
		//				AName:"",
		//				Expr: &NumberLiteral{Val: 0.2},
		//			},
		//		},
		//		TableName: "tbl",
		//	},
		//},

		{
			s: `SELECT name FROM tbl/*SELECT comment FROM testComments*/`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `/*SELECT comment FROM testComments*/SELECT name FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT name /*SELECT comment FROM testComments*/ FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT true AS f1, FALSE as f2 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{AName: "f1", Name: "", Expr: &BooleanLiteral{Val: true}},
					{AName: "f2", Name: "", Expr: &BooleanLiteral{Val: false}},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT true AS f1 FROM tbl WHERE f2 = true`,
			stmt: &SelectStatement{
				Fields: []Field{
					{AName: "f1", Name: "", Expr: &BooleanLiteral{Val: true}},
				},
				Sources:   []Source{&Table{Name: "tbl"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "f2"}, OP: EQ, RHS: &BooleanLiteral{Val: true}},
			},
		},

		{
			s: `SELECT indexof(field1, "abc") FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						AName: "",
						Name:  "indexof",
						Expr: &Call{
							Name: "indexof",
							Args: []Expr{&FieldRef{Name: "field1"}, &StringLiteral{Val: "abc"}},
						},
					},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		//The negative value expression support.
		{
			s: `SELECT -3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &IntegerLiteral{Val: -3},
						Name:  "",
						AName: "t1"},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT - 3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &IntegerLiteral{Val: -3},
						Name:  "",
						AName: "t1"},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT -. 3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &NumberLiteral{Val: -.3},
						Name:  "",
						AName: "t1"},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT -3.3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &NumberLiteral{Val: -3.3},
						Name:  "",
						AName: "t1"},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT sample(-.3,) FROM tbl`,
			stmt: nil,
			err:  "cannot get the plugin file path: invalid name sample: not exist",
		},

		{
			s:    `select timestamp() as tp from demo`,
			stmt: nil,
			err:  "found \"TIMESTAMP\", expected expression.",
		},

		{
			s: `select tstamp() as tp from demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &Call{
							Name: "tstamp",
							Args: nil,
						},
						Name:  "tstamp",
						AName: "tp"},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
			err: "",
		},

		{
			s:    "SELECT `half FROM tb",
			stmt: nil,
			err:  `found "EOF", expected FROM.`,
		},
		{
			s: "SELECT `space var` FROM tbl",
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "space var"},
						Name:  "space var",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},
		{
			s: "SELECT `中文 Chinese` FROM tbl",
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "中文 Chinese"},
						Name:  "中文 Chinese",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}

func TestParser_ParseWindowsExpr(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *SelectStatement
		err  string
	}{
		{
			s: `SELECT f1 FROM tbl GROUP BY TUMBLINGWINDOW(ss, 10)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "f1"},
						Name:  "f1",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: TUMBLING_WINDOW,
							Length:     &IntegerLiteral{Val: 10000},
							Interval:   &IntegerLiteral{Val: 0},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY HOPPINGWINDOW(mi, 5, 1)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "f1"},
						Name:  "f1",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: HOPPING_WINDOW,
							Length:     &IntegerLiteral{Val: 3e5},
							Interval:   &IntegerLiteral{Val: 6e4},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY SESSIONWINDOW(hh, 5, 1)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "f1"},
						Name:  "f1",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: SESSION_WINDOW,
							Length:     &IntegerLiteral{Val: 1.8e7},
							Interval:   &IntegerLiteral{Val: 3.6e6},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY SLIDINGWINDOW(ms, 5)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "f1"},
						Name:  "f1",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: SLIDING_WINDOW,
							Length:     &IntegerLiteral{Val: 5},
							Interval:   &IntegerLiteral{Val: 0},
						},
					},
				},
			},
		},

		{
			s:    `SELECT f1 FROM tbl GROUP BY SLIDINGWINDOW(mi, 5, 1)`,
			stmt: nil,
			err:  "The arguments for slidingwindow should be 2.\n",
		},

		{
			s:    `SELECT f1 FROM tbl GROUP BY SLIDINGWINDOW("mi", 5)`,
			stmt: nil,
			err:  "The 1st argument for slidingwindow is expecting timer literal expression. One value of [dd|hh|mi|ss|ms].\n",
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY COUNTWINDOW(10)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "f1"},
						Name:  "f1",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: COUNT_WINDOW,
							Length:     &IntegerLiteral{Val: 10},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY COUNTWINDOW(10, 5)`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{Name: "f1"},
						Name:  "f1",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: COUNT_WINDOW,
							Length:     &IntegerLiteral{Val: 10},
							Interval:   &IntegerLiteral{Val: 5},
						},
					},
				},
			},
		},

		{
			s:    `SELECT f1 FROM tbl GROUP BY COUNTWINDOW(3, 5)`,
			stmt: nil,
			err:  "The second parameter value 5 should be less than the first parameter 3.",
		},
		{
			s: `SELECT * FROM demo GROUP BY COUNTWINDOW(3,1) FILTER( where revenue > 100 )`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
				Dimensions: Dimensions{
					Dimension{
						Expr: &Window{
							WindowType: COUNT_WINDOW,
							Length:     &IntegerLiteral{Val: 3},
							Interval:   &IntegerLiteral{Val: 1},
							Filter: &BinaryExpr{
								LHS: &FieldRef{Name: "revenue"},
								OP:  GT,
								RHS: &IntegerLiteral{Val: 100},
							},
						},
					},
				},
			},
		},
		{
			s: `SELECT * FROM demo GROUP BY department, COUNTWINDOW(3,1) FILTER( where revenue > 100 ), year`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
				Dimensions: Dimensions{
					Dimension{Expr: &FieldRef{Name: "department"}},
					Dimension{
						Expr: &Window{
							WindowType: COUNT_WINDOW,
							Length:     &IntegerLiteral{Val: 3},
							Interval:   &IntegerLiteral{Val: 1},
							Filter: &BinaryExpr{
								LHS: &FieldRef{Name: "revenue"},
								OP:  GT,
								RHS: &IntegerLiteral{Val: 100},
							},
						},
					},
					Dimension{Expr: &FieldRef{Name: "year"}},
				},
			},
		},
		//to be supported
		{
			s:    `SELECT sum(f1) FILTER( where revenue > 100 ) FROM tbl GROUP BY year`,
			stmt: nil,
			err:  "found \"FILTER\", expected FROM.",
		},
		{
			s:    `SELECT * FROM demo GROUP BY COUNTWINDOW(3,1) FILTER where revenue > 100`,
			stmt: nil,
			err:  "Found \"WHERE\" after FILTER, expect parentheses.",
		},
		{
			s:    `SELECT * FROM demo GROUP BY COUNTWINDOW(3,1) where revenue > 100`,
			stmt: nil,
			err:  "found \"WHERE\", expected EOF.",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}

func TestParser_ParseJsonExpr(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *SelectStatement
		err  string
	}{
		{
			s: `SELECT children[0] FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "children"},
							OP:  SUBSET,
							RHS: &IndexExpr{Index: 0},
						},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[0]->first FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{
								LHS: &FieldRef{Name: "children"},
								OP:  SUBSET,
								RHS: &IndexExpr{Index: 0},
							},
							OP:  ARROW,
							RHS: &FieldRef{Name: "first"},
						},

						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children->first[2] FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{
								LHS: &FieldRef{Name: "children"},
								OP:  ARROW,
								RHS: &FieldRef{Name: "first"},
							},
							OP:  SUBSET,
							RHS: &IndexExpr{Index: 2},
						},

						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children->first[2]->test FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{
								LHS: &BinaryExpr{
									LHS: &FieldRef{Name: "children"},
									OP:  ARROW,
									RHS: &FieldRef{Name: "first"},
								},
								OP:  SUBSET,
								RHS: &IndexExpr{Index: 2},
							},
							OP:  ARROW,
							RHS: &FieldRef{Name: "test"},
						},

						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[0:1] FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "children"},
							OP:  SUBSET,
							RHS: &ColonExpr{Start: 0, End: 1},
						},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[:1] FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "children"},
							OP:  SUBSET,
							RHS: &ColonExpr{Start: 0, End: 1},
						},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[:] FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "children"},
							OP:  SUBSET,
							RHS: &ColonExpr{Start: 0, End: -1},
						},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[2:] AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "children"},
							OP:  SUBSET,
							RHS: &ColonExpr{Start: 2, End: -1},
						},
						Name:  "",
						AName: "c"},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[2:]->first AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{LHS: &FieldRef{Name: "children"}, OP: SUBSET, RHS: &ColonExpr{Start: 2, End: -1}},
							OP:  ARROW,
							RHS: &FieldRef{Name: "first"},
						},
						Name:  "",
						AName: "c"},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT demo.* FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("demo"), Name: "*"},
						Name:  "*",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT demo.children[2:]->first AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{LHS: &FieldRef{StreamName: StreamName("demo"), Name: "children"}, OP: SUBSET, RHS: &ColonExpr{Start: 2, End: -1}},
							OP:  ARROW,
							RHS: &FieldRef{Name: "first"},
						},
						Name:  "",
						AName: "c"},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT lower(demo.children[2:]->first) AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &Call{
							Name: "lower",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{LHS: &FieldRef{StreamName: StreamName("demo"), Name: "children"}, OP: SUBSET, RHS: &ColonExpr{Start: 2, End: -1}},
									OP:  ARROW,
									RHS: &FieldRef{Name: "first"},
								},
							},
						},
						Name:  "lower",
						AName: "c"},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[:1] FROM demo WHERE abc[0] > 12`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "children"},
							OP:  SUBSET,
							RHS: &ColonExpr{Start: 0, End: 1},
						},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{
						LHS: &FieldRef{Name: "abc"},
						OP:  SUBSET,
						RHS: &IndexExpr{Index: 0},
					},
					OP:  GT,
					RHS: &IntegerLiteral{Val: 12},
				},
			},
		},

		{
			s:    `SELECT demo.children.first AS c FROM demo`,
			stmt: nil,
			err:  "Too many field names. Please use -> to reference keys in struct.\n",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}

func TestParser_ParseJoins(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *SelectStatement
		err  string
	}{
		{
			s: `SELECT * FROM topic/sensor1 LEFT JOIN topic1 ON f=k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1"}},
				Joins: []Join{
					{
						Name: "topic1", Alias: "", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{Name: "k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 AS t1 INNER JOIN topic1 AS t2 ON f=k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1", Alias: "t2", JoinType: INNER_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{Name: "k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 AS t1 LEFT JOIN topic1/sensor2 AS t2 ON f=k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &Wildcard{Token: ASTERISK},
						Name:  "",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1/sensor2", Alias: "t2", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{Name: "k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 LEFT JOIN topic1/sensor2 AS t2 ON f=k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1/sensor2", Alias: "t2", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{Name: "k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 LEFT JOIN topic1/sensor2 AS t2 ON t1.f=t2.k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1/sensor2", Alias: "t2", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{StreamName: StreamName("t1"), Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{StreamName: StreamName("t2"), Name: "k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 RIGHT JOIN topic1/sensor2 AS t2 ON t1.f=t2.k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1/sensor2", Alias: "t2", JoinType: RIGHT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{StreamName: StreamName("t1"), Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{StreamName: StreamName("t2"), Name: "k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 FULL JOIN topic1/sensor2 AS t2 ON t1.f=t2.k`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1/sensor2", Alias: "t2", JoinType: FULL_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{StreamName: StreamName("t1"), Name: "f"},
							OP:  EQ,
							RHS: &FieldRef{StreamName: StreamName("t2"), Name: "k"},
						},
					},
				},
			},
		},

		{
			s:    `SELECT t1.name FROM topic/sensor1 AS t1 CROSS JOIN topic1/sensor2 AS t2 ON t1.f=t2.k`,
			stmt: nil,
			err:  "On expression is not required for cross join type.\n",
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 CROSS JOIN topic1/sensor2 AS t2`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name:  "name",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "topic/sensor1", Alias: "t1"}},
				Joins: []Join{
					{
						Name: "topic1/sensor2", Alias: "t2", JoinType: CROSS_JOIN, Expr: nil,
					},
				},
			},
		},

		{
			s: `SELECT demo.*, demo2.* FROM demo LEFT JOIN demo2 on demo.f1 = demo2.f2`,
			stmt: &SelectStatement{
				Fields: []Field{
					{
						Expr:  &FieldRef{StreamName: StreamName("demo"), Name: "*"},
						Name:  "*",
						AName: ""},
					{
						Expr:  &FieldRef{StreamName: StreamName("demo2"), Name: "*"},
						Name:  "*",
						AName: ""},
				},
				Sources: []Source{&Table{Name: "demo"}},
				Joins: []Join{
					{
						Name: "demo2", Alias: "", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{StreamName: StreamName("demo"), Name: "f1"},
							OP:  EQ,
							RHS: &FieldRef{StreamName: StreamName("demo2"), Name: "f2"},
						},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}

func TestParser_ParseStatements(t *testing.T) {
	var tests = []struct {
		s     string
		stmts SelectStatements
		err   string
	}{
		{s: "SELECT name FROM tbl;\nSELECT name FROM topic/sensor1\n",
			stmts: []SelectStatement{
				{
					Fields: []Field{
						{
							Expr:  &FieldRef{Name: "name"},
							Name:  "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name: "tbl"}},
				},
				{
					Fields: []Field{
						{
							Expr:  &FieldRef{Name: "name"},
							Name:  "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name: "topic/sensor1"}},
				},
			},
		},
		{s: "SELECT name FROM tbl;\nSELECT name FROM topic/sensor1;\n--SELECT comment FROM topic/comment",
			stmts: []SelectStatement{
				{
					Fields: []Field{
						{
							Expr:  &FieldRef{Name: "name"},
							Name:  "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name: "tbl"}},
				},
				{
					Fields: []Field{
						{
							Expr:  &FieldRef{Name: "name"},
							Name:  "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name: "topic/sensor1"}},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmts, err := NewParser(strings.NewReader(tt.s)).ParseQueries()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmts, stmts) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmts, stmts)
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
