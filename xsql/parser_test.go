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
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{StreamName: StreamName("t1"), Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1 AS t1`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1/#`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1/#"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1/# AS t2 `,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1/#", Alias:"t2"}},
			},
		},

		{
			s: `SELECT name FROM /topic/sensor1/#`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"/topic/sensor1/#"}},
			},
		},

		{
			s: `SELECT name FROM /topic/sensor1/#/`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"/topic/sensor1/#/"}},
			},
		},

		{
			s: `SELECT name FROM /topic/sensor1/+/temp1/`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"/topic/sensor1/+/temp1/"}},
			},
		},

		{
			s: `SELECT name FROM topic/sensor1/+/temp`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1/+/temp"}},
			},
		},

		{
			s: `SELECT * FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},
		{
			s: `SELECT a,b FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{Expr:&FieldRef{Name: "a"}, Name: "a", AName:""},
					Field{Expr:&FieldRef{Name: "b"}, Name: "b", AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},
		{
			s: `SELECT a, b,c FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{Expr:&FieldRef{Name: "a"}, Name: "a", AName:""},
					Field{Expr:&FieldRef{Name: "b"}, Name: "b", AName:""},
					Field{Expr:&FieldRef{Name: "c"}, Name: "c", AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT a AS alias FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{Field{Expr:&FieldRef{Name: "a"}, Name: "a", AName:"alias"},},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT a AS alias1, b as Alias2 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{Expr: &FieldRef{Name:"a"}, Name: "a", AName:"alias1"},
					Field{Expr: &FieldRef{Name:"b"}, Name: "b", AName:"Alias2"},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample0("test") FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample0",
						Expr:&Call{
							Name:"sample0",
							Args: []Expr{&StringLiteral{Val:"test"}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample1(test) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample1",
						Expr:&Call{
							Name:"sample1",
							Args: []Expr{&FieldRef{Name:"test"}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},


		{
			s: `SELECT sample2(123) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample2",
						Expr:&Call{
							Name:"sample2",
							Args: []Expr{&IntegerLiteral{Val:123}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample3(123, "abc") FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample3",
						Expr:&Call{
							Name:"sample3",
							Args: []Expr{&IntegerLiteral{Val:123}, &StringLiteral{Val:"abc"}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample3() FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample3",
						Expr:&Call{
							Name:"sample3",
							Args: nil ,
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample4("abc", 1234, field1) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample4",
						Expr:&Call{
							Name:"sample4",
							Args: []Expr{
								&StringLiteral{Val:"abc"},
								&IntegerLiteral{Val:1234},
								&FieldRef{Name:"field1"},
							} ,
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample2(sample1(test),1) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample2",
						Expr:&Call{
							Name:"sample2",
							Args: []Expr{
								&Call{
									Name:"sample1",
									Args:[]Expr{
										&FieldRef{Name: "test"},
									},
								},
								&IntegerLiteral{Val:1},
							},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample2(sample1(test),1) AS field1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"field1",
						Name: "sample2",
						Expr:&Call{
							Name:"sample2",
							Args: []Expr{
								&Call{
									Name:"sample1",
									Args:[]Expr{
										&FieldRef{Name: "test"},
									},
								},
								&IntegerLiteral{Val:1},
							},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample2(sample1("test")) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample2",
						Expr:&Call{
							Name:"sample2",
							Args: []Expr{
								&Call{
									Name:"sample1",
									Args:[]Expr{
										&StringLiteral{Val: "test"},
									},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},


		{
			s: `SELECT "abc" FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "", Expr: &StringLiteral{Val:"abc"}}},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT "abc" AS field1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"field1", Name: "", Expr: &StringLiteral{Val:"abc"}}},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT field0,   "abc" AS field1, field2 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{AName:"", Name: "field0", Expr: &FieldRef{Name:"field0"}},
					Field{AName:"field1", Name: "", Expr: &StringLiteral{Val:"abc"}},
					Field{AName:"", Name: "field2", Expr: &FieldRef{Name:"field2"}},},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT * AS alias FROM tbl`,
			stmt: nil,
			err: `found "AS", expected FROM.`,
		},

		{
			s: `SELECT *, FROM tbl`,
			stmt: nil,
			err: `found ",", expected FROM.`,
		},

		{
			s: `SELECTname FROM tbl`,
			stmt: nil,
			err: `Found "SELECTname", Expected SELECT.` + "\n",
		},

		{
			s: `SELECT abc FROM tbl WHERE abc > 12 `,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "abc", Expr: &FieldRef{Name:"abc"}}},
				Sources: []Source{&Table{Name:"tbl"}},
				Condition: &BinaryExpr{
					LHS:&FieldRef{Name:"abc"},
					OP:GT,
					RHS:&IntegerLiteral{Val:12},
				},
			},
		},

		{
			s: `SELECT abc FROM tbl WHERE abc = "hello" `,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "abc", Expr: &FieldRef{Name:"abc"}}},
				Sources: []Source{&Table{Name:"tbl"}},
				Condition:  &BinaryExpr{
					LHS:&FieldRef{Name:"abc"},
					OP:EQ,
					RHS:&StringLiteral{Val:"hello"},
				},
			},
		},

		{
			s: `SELECT t1.abc FROM tbl AS t1 WHERE t1.abc = "hello" `,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "abc", Expr: &FieldRef{StreamName:StreamName("t1"), Name:"abc"}}},
				Sources: []Source{&Table{Name:"tbl", Alias:"t1"}},
				Condition:  &BinaryExpr{
					LHS:&FieldRef{StreamName:StreamName("t1"), Name:"abc"},
					OP:EQ,
					RHS:&StringLiteral{Val:"hello"},
				},
			},
		},

		{
			s: `SELECT abc, "fff" AS fa FROM tbl WHERE fa >= 5 `,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "abc", Expr: &FieldRef{Name:"abc"}}, Field{AName:"fa", Name: "", Expr: &StringLiteral{Val:"fff"}}},
				Sources: []Source{&Table{Name:"tbl"}},
				Condition:  &BinaryExpr{
					LHS:&FieldRef{Name:"fa"},
					OP:GTE,
					RHS:&IntegerLiteral{Val:5},
				},
			},
		},

		{
			s: `SELECT field2 FROM tbl WHERE field2 != 5 `,
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "field2", Expr: &FieldRef{Name:"field2"}}, },
				Sources: []Source{&Table{Name:"tbl"}},
				Condition:  &BinaryExpr{
					LHS:&FieldRef{Name:"field2"},
					OP:NEQ,
					RHS:&IntegerLiteral{Val:5},
				},
			},
		},

		{
			s: `SELECT field2 FROM tbl WHERE field2 !   = 5 `, //Add space char in expression
			stmt: &SelectStatement{
				Fields:    []Field{Field{AName:"", Name: "field2", Expr: &FieldRef{Name:"field2"}}, },
				Sources: []Source{&Table{Name:"tbl"}},
				Condition:  &BinaryExpr{
					LHS:&FieldRef{Name:"field2"},
					OP:NEQ,
					RHS:&IntegerLiteral{Val:5},
				},
			},
		},

		{
			s: `SELECT *f FROM tbl`,
			stmt: nil,
			err: `found "f", expected FROM.`,
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
				Fields:    []Field{
					Field{
						AName:"",
						Name: "",
						Expr: &BinaryExpr{
							LHS:&FieldRef{Name:"abc"},
							OP:ADD,
							RHS:&IntegerLiteral{Val:2},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT t1.abc+2 FROM tbl AS t1`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "",
						Expr: &BinaryExpr{
							LHS:&FieldRef{StreamName:StreamName("t1"), Name:"abc"},
							OP:ADD,
							RHS:&IntegerLiteral{Val:2},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl", Alias:"t1"}},
			},
		},

		{
			s: `SELECT abc + "hello" FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "",
						Expr: &BinaryExpr{
							LHS:&FieldRef{Name:"abc"},
							OP:ADD,
							RHS:&StringLiteral{Val:"hello"},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT abc*2 + 3 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "",
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{
								LHS:&FieldRef{Name:"abc"},
								OP:MUL,
								RHS:&IntegerLiteral{Val:2},
							},
							OP:ADD,
							RHS:&IntegerLiteral{Val:3},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample(abc*2 + 3) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample",
						Expr: &Call{
							Name:"sample",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{
										LHS:&FieldRef{Name:"abc"},
										OP:MUL,
										RHS:&IntegerLiteral{Val:2},
									},
									OP:ADD,
									RHS:&IntegerLiteral{Val:3},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample(t1.abc*2 + 3) FROM tbl AS t1`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample",
						Expr: &Call{
							Name:"sample",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{
										LHS:&FieldRef{StreamName:StreamName("t1"), Name:"abc"},
										OP:MUL,
										RHS:&IntegerLiteral{Val:2},
									},
									OP:ADD,
									RHS:&IntegerLiteral{Val:3},
								},
							},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl", Alias:"t1"}},
			},
		},

		{
			s: `SELECT sample(abc*2 + 3, "param2") FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample",
						Expr: &Call{
							Name:"sample",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{
										LHS:&FieldRef{Name:"abc"},
										OP:MUL,
										RHS:&IntegerLiteral{Val:2},
									},
									OP:ADD,
									RHS:&IntegerLiteral{Val:3},
								},
								&StringLiteral{Val:"param2"},
							},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT 0.2 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "",
						Expr: &NumberLiteral{Val: 0.2},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT .2 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "",
						Expr: &NumberLiteral{Val: 0.2},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample(.2) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample",
						Expr:&Call{
							Name:"sample",
							Args: []Expr{&NumberLiteral{Val:0.2}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample(.2, "abc") FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample",
						Expr:&Call{
							Name:"sample",
							Args: []Expr{&NumberLiteral{Val:0.2}, &StringLiteral{Val: "abc"}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample(.2, "abc") AS f1 FROM tbl WHERE f1 > 2.2`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"f1",
						Name: "sample",
						Expr:&Call{
							Name:"sample",
							Args: []Expr{&NumberLiteral{Val:0.2}, &StringLiteral{Val: "abc"}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
				Condition: &BinaryExpr{
					LHS: &FieldRef{Name:"f1"},
					OP: GT,
					RHS: &NumberLiteral{Val:2.2},
				},
			},
		},

		{
			s: `SELECT deviceId, name FROM topic/sensor1 WHERE deviceId=1 AND name = "dname"`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "deviceId"}, Name: "deviceId", AName:""},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{LHS: &FieldRef{Name: "deviceId"}, OP: EQ, RHS: &IntegerLiteral{Val: 1},},
					OP:  AND,
					RHS: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				},
			},
		},

		{
			s: `SELECT deviceId, name FROM topic/sensor1 AS t1 WHERE t1.deviceId=1 AND t1.name = "dname"`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "deviceId"}, Name: "deviceId", AName:""},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{LHS: &FieldRef{StreamName:StreamName("t1"), Name: "deviceId"}, OP: EQ, RHS: &IntegerLiteral{Val: 1},},
					OP:  AND,
					RHS: &BinaryExpr{LHS: &FieldRef{StreamName:StreamName("t1"), Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				},
			},
		},


		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE t> = 20.5 OR name = "dname"`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{LHS: &FieldRef{Name: "t"}, OP: GTE, RHS: &NumberLiteral{Val: 20.5},},
					OP:  OR,
					RHS: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				},
			},
		},


		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY name`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{Dimension{Expr:&FieldRef{Name:"name"}}},
			},
		},

		{
			s: `SELECT s1.temp AS t, name FROM topic/sensor1 AS s1 WHERE t = "dname" GROUP BY s1.temp`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{StreamName:"s1", Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"s1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "t"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{Dimension{Expr:&FieldRef{StreamName:"s1", Name: "temp"}}},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY sample(name,1)`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{Dimension{
						Expr:&Call{Name:"sample", Args:[]Expr{&FieldRef{Name:"name"}, &IntegerLiteral{Val:1}}},
					},
				},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 AS s1 WHERE name = "dname" GROUP BY sample(s1.name,1)`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"s1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{Dimension{
					Expr:&Call{Name:"sample", Args:[]Expr{&FieldRef{StreamName:StreamName("s1"), Name:"name"}, &IntegerLiteral{Val:1}}},
				},
				},
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY sample(name,1) ORDER BY name`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{
					Dimension{
						Expr:&Call{Name:"sample", Args:[]Expr{
							&FieldRef{Name:"name"},
							&IntegerLiteral{Val:1}},
						},
					},
				},
				SortFields: []SortField {SortField{Name:"name", Ascending:true}, },
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 AS s1 WHERE s1.name = "dname" GROUP BY sample(s1.name,1) ORDER BY s1.name`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"s1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{StreamName:StreamName("s1"), Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{
					Dimension{
						Expr:&Call{Name:"sample", Args:[]Expr{
							&FieldRef{StreamName:StreamName("s1"), Name:"name"},
							&IntegerLiteral{Val:1}},
						},
					},
				},
				SortFields: []SortField {SortField{Name:"s1.name", Ascending:true}, },
			},
		},

		{
			s: `SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY sample(name,1) ORDER BY name DESC`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{ Expr:&FieldRef{Name: "temp"}, Name: "temp", AName:"t"},
					Field{ Expr:&FieldRef{Name: "name"}, Name: "name", AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "name"}, OP: EQ, RHS: &StringLiteral{Val: "dname"},},
				Dimensions:Dimensions{
					Dimension{
						Expr:&Call{Name:"sample", Args:[]Expr{
							&FieldRef{Name:"name"},
							&IntegerLiteral{Val:1}},
						},
					},
				},
				SortFields: []SortField {SortField{Name:"name", Ascending:false}, },
			},
		},


		{
			s: `SELECT * FROM topic/sensor1 ORDER BY name DESC`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				SortFields: []SortField {SortField{Name:"name", Ascending:false}, },
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 ORDER BY name DESC, name2 ASC`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				SortFields: []SortField {SortField{Name:"name", Ascending:false}, SortField{Name:"name2", Ascending:true},},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 GROUP BY name, name2,sample(name3,1.8) ORDER BY name DESC, name2 ASC`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Dimensions: Dimensions{
					Dimension{Expr:&FieldRef{Name:"name"}},
					Dimension{Expr:&FieldRef{Name:"name2"}},
					Dimension{
						Expr:&Call{Name:"sample", Args:[]Expr{
							&FieldRef{Name:"name3"},
							&NumberLiteral{Val:1.8}},
						},
					},
				},
				SortFields: []SortField {SortField{Name:"name", Ascending:false}, SortField{Name:"name2", Ascending:true},},
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
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `/*SELECT comment FROM testComments*/SELECT name FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT name /*SELECT comment FROM testComments*/ FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&FieldRef{Name: "name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT true AS f1, FALSE as f2 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{AName:"f1", Name: "", Expr: &BooleanLiteral{Val:true}},
					Field{AName:"f2", Name: "", Expr: &BooleanLiteral{Val:false}},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT true AS f1 FROM tbl WHERE f2 = true`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{AName:"f1", Name: "", Expr: &BooleanLiteral{Val:true}},
				},
				Sources: []Source{&Table{Name:"tbl"}},
				Condition: &BinaryExpr{LHS: &FieldRef{Name: "f2"}, OP: EQ, RHS: &BooleanLiteral{Val: true},},
			},
		},

		{
			s: `SELECT sample(true, "abc") FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						AName:"",
						Name: "sample",
						Expr:&Call{
							Name:"sample",
							Args: []Expr{&BooleanLiteral{Val:true}, &StringLiteral{Val: "abc"}},
						},
					},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		//The negative value expression support.
		{
			s: `SELECT -3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&IntegerLiteral{Val:-3},
						Name: "",
						AName:"t1"},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT - 3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&IntegerLiteral{Val:-3},
						Name: "",
						AName:"t1"},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT -. 3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&NumberLiteral{Val:-.3},
						Name: "",
						AName:"t1"},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT -3.3 AS t1 FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&NumberLiteral{Val:-3.3},
						Name: "",
						AName:"t1"},
				},
				Sources: []Source{&Table{Name:"tbl"}},
			},
		},

		{
			s: `SELECT sample(-.3,) FROM tbl`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr:&Call{
							Name:"sample",
							Args: []Expr {
								&NumberLiteral{Val:-0.3},
							},
						},
						Name: "sample",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
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
				Fields:    []Field{
					Field{
						Expr: &FieldRef{Name:"f1"},
						Name: "f1",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr:&Windows{
							WindowType: TUMBLING_WINDOW,
							Args:[]Expr{&TimeLiteral{Val:SS}, &IntegerLiteral{Val:10}},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY HOPPINGWINDOW(mi, 5, 1)`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &FieldRef{Name:"f1"},
						Name: "f1",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr:&Windows{
							WindowType: HOPPING_WINDOW,
							Args:[]Expr{ &TimeLiteral{Val:MI}, &IntegerLiteral{Val:5}, &IntegerLiteral{Val:1}},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY SESSIONWINDOW(hh, 5, 1)`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &FieldRef{Name:"f1"},
						Name: "f1",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr:&Windows{
							WindowType: SESSION_WINDOW,
							Args:[]Expr{ &TimeLiteral{Val:HH}, &IntegerLiteral{Val:5}, &IntegerLiteral{Val:1}},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY SLIDINGWINDOW(ms, 5)`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &FieldRef{Name:"f1"},
						Name: "f1",
						AName:""},
				},
				Sources: []Source{&Table{Name:"tbl"}},
				Dimensions: Dimensions{
					Dimension{
						Expr:&Windows{
							WindowType: SLIDING_WINDOW,
							Args:[]Expr{ &TimeLiteral{Val:MS}, &IntegerLiteral{Val:5}},
						},
					},
				},
			},
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY SLIDINGWINDOW(mi, 5, 1)`,
			stmt: nil,
			err: "The arguments for slidingwindow should be 2.\n",
		},

		{
			s: `SELECT f1 FROM tbl GROUP BY SLIDINGWINDOW("mi", 5)`,
			stmt: nil,
			err: "The 1st argument for slidingwindow is expecting timer literal expression. One value of [dd|hh|mi|ss|ms].\n",
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
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"children"},
							OP: SUBSET,
							RHS: &IndexExpr{Index:0},
						},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},
			},
		},

		{
			s: `SELECT children[0]->first FROM demo`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS:&BinaryExpr{
								LHS: &FieldRef{Name:"children"},
								OP: SUBSET,
								RHS: &IndexExpr{Index:0},
							},
							OP: ARROW,
							RHS: &FieldRef{Name:"first"},
						},

						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},

			},
		},

		{
			s: `SELECT children->first[2] FROM demo`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS:&BinaryExpr{
								LHS: &FieldRef{Name:"children"},
								OP: ARROW,
								RHS: &FieldRef{Name:"first"},
							},
							OP: SUBSET,
							RHS: &IndexExpr{Index:2},
						},

						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},

			},
		},

		{
			s: `SELECT children->first[2]->test FROM demo`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
								LHS:&BinaryExpr{
									LHS:&BinaryExpr{
										LHS: &FieldRef{Name:"children"},
										OP: ARROW,
										RHS: &FieldRef{Name:"first"},
									},
									OP: SUBSET,
									RHS: &IndexExpr{Index:2},
								},
								OP: ARROW,
								RHS:&FieldRef{Name:"test"},
						},

						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},

			},
		},

		{
			s: `SELECT children[0:1] FROM demo`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"children"},
							OP: SUBSET,
							RHS: &ColonExpr{Start:0, End:1},
						},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},
			},
		},

		{
			s: `SELECT children[:1] FROM demo`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"children"},
							OP: SUBSET,
							RHS: &ColonExpr{Start:0, End:1},
						},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},
			},
		},

		{
			s: `SELECT children[:] FROM demo`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"children"},
							OP: SUBSET,
							RHS: &ColonExpr{Start:0, End:-1},
						},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},
			},
		},

		{
			s: `SELECT children[2:] AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					Field{
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
					Field{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{LHS: &FieldRef{Name: "children"}, OP: SUBSET, RHS: &ColonExpr{Start: 2, End: -1},
							},
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
			s: `SELECT demo.children[2:]->first AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					Field{
						Expr: &BinaryExpr{
							LHS: &BinaryExpr{LHS: &FieldRef{StreamName:StreamName("demo"), Name: "children"}, OP: SUBSET, RHS: &ColonExpr{Start: 2, End: -1},
							},
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
			s: `SELECT sample(demo.children[2:]->first) AS c FROM demo`,
			stmt: &SelectStatement{
				Fields: []Field{
					Field{
						Expr:&Call{
							Name:"sample",
							Args: []Expr{
								&BinaryExpr{
									LHS: &BinaryExpr{LHS: &FieldRef{StreamName:StreamName("demo"), Name: "children"}, OP: SUBSET, RHS: &ColonExpr{Start: 2, End: -1},
									},
									OP:  ARROW,
									RHS: &FieldRef{Name: "first"},
								},
							},
						},
						Name:  "sample",
						AName: "c"},
				},
				Sources: []Source{&Table{Name: "demo"}},
			},
		},

		{
			s: `SELECT children[:1] FROM demo WHERE abc[0] > 12`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"children"},
							OP: SUBSET,
							RHS: &ColonExpr{Start:0, End:1},
						},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"demo"}},
				Condition: &BinaryExpr{
					LHS: &BinaryExpr{
						LHS: &FieldRef{Name:"abc"},
						OP: SUBSET,
						RHS: &IndexExpr{Index:0},
					},
					OP:GT,
					RHS:&IntegerLiteral{Val:12},
				},
			},
		},

		{
			s: `SELECT demo.children.first AS c FROM demo`,
			stmt: nil,
			err: "Too many field names. Please use -> to reference keys in struct.\n",
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
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1"}},
				Joins: []Join{
					Join{
						Name:"topic1", Alias: "", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"f"},
							OP: EQ,
							RHS:&FieldRef{Name:"k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 AS t1 INNER JOIN topic1 AS t2 ON f=k`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
				Joins: []Join{
					Join{
						Name:"topic1", Alias: "t2", JoinType: INNER_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"f"},
							OP: EQ,
							RHS:&FieldRef{Name:"k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT * FROM topic/sensor1 AS t1 LEFT JOIN topic1/sensor2 AS t2 ON f=k`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &Wildcard{Token: ASTERISK},
						Name: "",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
				Joins: []Join{
					Join{
						Name:"topic1/sensor2", Alias: "t2", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"f"},
							OP: EQ,
							RHS:&FieldRef{Name:"k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 LEFT JOIN topic1/sensor2 AS t2 ON f=k`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &FieldRef{StreamName:StreamName("t1"), Name:"name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
				Joins: []Join{
					Join{
						Name:"topic1/sensor2", Alias: "t2", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{Name:"f"},
							OP: EQ,
							RHS:&FieldRef{Name:"k"},
						},
					},
				},
			},
		},

		{
			s: `SELECT t1.name FROM topic/sensor1 AS t1 LEFT JOIN topic1/sensor2 AS t2 ON t1.f=t2.k`,
			stmt: &SelectStatement{
				Fields:    []Field{
					Field{
						Expr: &FieldRef{StreamName:StreamName("t1"), Name:"name"},
						Name: "name",
						AName:""},
				},
				Sources: []Source{&Table{Name:"topic/sensor1", Alias:"t1"}},
				Joins: []Join{
					Join{
						Name:"topic1/sensor2", Alias: "t2", JoinType: LEFT_JOIN, Expr: &BinaryExpr{
							LHS: &FieldRef{StreamName:StreamName("t1"), Name:"f"},
							OP: EQ,
							RHS:&FieldRef{StreamName:StreamName("t2"), Name:"k"},
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
		s    string
		stmts SelectStatements
		err  string
	}{
		{s: "SELECT name FROM tbl;\nSELECT name FROM topic/sensor1\n",
			stmts: []SelectStatement{
				SelectStatement{
					Fields: []Field{
						Field{
							Expr:  &FieldRef{Name: "name"},
							Name: "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name:"tbl"}},
				},
				SelectStatement{
					Fields: []Field{
						Field{
							Expr:  &FieldRef{Name: "name"},
							Name: "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name:"topic/sensor1"}},
				},
			},
		},
		{s: "SELECT name FROM tbl;\nSELECT name FROM topic/sensor1;\n--SELECT comment FROM topic/comment",
			stmts: []SelectStatement{
				SelectStatement{
					Fields: []Field{
						Field{
							Expr:  &FieldRef{Name: "name"},
							Name: "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name:"tbl"}},
				},
				SelectStatement{
					Fields: []Field{
						Field{
							Expr:  &FieldRef{Name: "name"},
							Name: "name",
							AName: ""},
					},
					Sources: []Source{&Table{Name:"topic/sensor1"}},
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
