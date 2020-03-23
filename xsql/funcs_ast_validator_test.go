package xsql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Ensure the parser can parse strings into Statement ASTs.
func TestFuncValidator(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *SelectStatement
		err  string
	}{
		{
			s: `SELECT abs(1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "abs", Expr: &Call{Name: "abs", Args: []Expr{&IntegerLiteral{Val: 1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT abs(field1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "abs", Expr: &Call{Name: "abs", Args: []Expr{&FieldRef{Name: "field1"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT abs(1,2) FROM tbl`,
			stmt: nil,
			err:  "The arguments for abs should be 1.",
		},

		{
			s: `SELECT abs(1.1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "abs", Expr: &Call{Name: "abs", Args: []Expr{&NumberLiteral{Val: 1.1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT abs(true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function abs.",
		},

		{
			s:    `SELECT abs("test") FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function abs.",
		},

		{
			s:    `SELECT abs(ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function abs.",
		},

		///
		{
			s: `SELECT sin(1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "sin", Expr: &Call{Name: "sin", Args: []Expr{&IntegerLiteral{Val: 1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT sin(1.1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "sin", Expr: &Call{Name: "sin", Args: []Expr{&NumberLiteral{Val: 1.1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT sin(true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function sin.",
		},

		{
			s:    `SELECT sin("test") FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function sin.",
		},

		{
			s:    `SELECT sin(ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function sin.",
		},
		///
		{
			s: `SELECT tanh(1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "tanh", Expr: &Call{Name: "tanh", Args: []Expr{&IntegerLiteral{Val: 1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT tanh(1.1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "tanh", Expr: &Call{Name: "tanh", Args: []Expr{&NumberLiteral{Val: 1.1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT tanh(true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function tanh.",
		},

		{
			s:    `SELECT tanh("test") FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function tanh.",
		},

		{
			s:    `SELECT tanh(ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function tanh.",
		},

		///
		{
			s: `SELECT bitxor(1, 2) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "bitxor", Expr: &Call{Name: "bitxor", Args: []Expr{&IntegerLiteral{Val: 1}, &IntegerLiteral{Val: 2}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT bitxor(1.1, 2) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 1 parameter of function bitxor.",
		},

		{
			s:    `SELECT bitxor(true, 2) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 1 parameter of function bitxor.",
		},

		{
			s:    `SELECT bitxor(1, ss) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 2 parameter of function bitxor.",
		},

		{
			s:    `SELECT bitxor(1, 2.2) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 2 parameter of function bitxor.",
		},

		///
		{
			s: `SELECT bitnot(1) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "bitnot", Expr: &Call{Name: "bitnot", Args: []Expr{&IntegerLiteral{Val: 1}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT bitnot(1.1) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 1 parameter of function bitnot.",
		},

		{
			s:    `SELECT bitnot(true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 1 parameter of function bitnot.",
		},

		///
		{
			s: `SELECT mod(1, 2) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "mod", Expr: &Call{Name: "mod", Args: []Expr{&IntegerLiteral{Val: 1}, &IntegerLiteral{Val: 2}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT mod("1.1", 2) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 1 parameter of function mod.",
		},

		{
			s:    `SELECT mod(1.1, true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 2 parameter of function mod.",
		},

		{
			s:    `SELECT mod(1, ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for 2 parameter of function mod.",
		},

		///
		{
			s: `SELECT concat(field, "hello") FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "concat", Expr: &Call{Name: "concat", Args: []Expr{&FieldRef{Name: "field"}, &StringLiteral{Val: "hello"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT concat("1.1", 2) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 2 parameter of function concat.",
		},

		{
			s:    `SELECT concat("1.1", true) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 2 parameter of function concat.",
		},

		{
			s:    `SELECT concat("1", ss) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 2 parameter of function concat.",
		},

		///
		{
			s: `SELECT regexp_matches(field, "hello") FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "regexp_matches", Expr: &Call{Name: "regexp_matches", Args: []Expr{&FieldRef{Name: "field"}, &StringLiteral{Val: "hello"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT regexp_matches(1, "true") FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 1 parameter of function regexp_matches.",
		},

		{
			s:    `SELECT regexp_matches("1.1", 2) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 2 parameter of function regexp_matches.",
		},

		///
		{
			s: `SELECT regexp_replace(field, "hello", "h") FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "regexp_replace", Expr: &Call{Name: "regexp_replace", Args: []Expr{&FieldRef{Name: "field"}, &StringLiteral{Val: "hello"}, &StringLiteral{Val: "h"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT regexp_replace(field1, "true", true) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 3 parameter of function regexp_replace.",
		},

		///
		{
			s: `SELECT trim(field) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "trim", Expr: &Call{Name: "trim", Args: []Expr{&FieldRef{Name: "field"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT trim(1) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 1 parameter of function trim.",
		},

		///
		{
			s: `SELECT rpad(field, 3) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "rpad", Expr: &Call{Name: "rpad", Args: []Expr{&FieldRef{Name: "field"}, &IntegerLiteral{Val: 3}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT rpad("ff", true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 2 parameter of function rpad.",
		},

		///
		{
			s: `SELECT substring(field, 3, 4) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "substring", Expr: &Call{Name: "substring", Args: []Expr{&FieldRef{Name: "field"}, &IntegerLiteral{Val: 3}, &IntegerLiteral{Val: 4}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT substring(field, -1, 4) FROM tbl`,
			stmt: nil,
			err:  "The start index should not be a nagtive integer.",
		},

		{
			s:    `SELECT substring(field, 0, -1) FROM tbl`,
			stmt: nil,
			err:  "The end index should be larger than start index.",
		},

		{
			s:    `SELECT substring(field, 0, true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 3 parameter of function substring.",
		},

		///
		{
			s: `SELECT cast(field, "bigint") FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "cast", Expr: &Call{Name: "cast", Args: []Expr{&FieldRef{Name: "field"}, &StringLiteral{Val: "bigint"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT cast("12", "bool") FROM tbl`,
			stmt: nil,
			err:  "Expect one of following value for the 2nd parameter: bigint, float, string, boolean, datetime.",
		},

		///
		{
			s: `SELECT chr(field) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "chr", Expr: &Call{Name: "chr", Args: []Expr{&FieldRef{Name: "field"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT chr(true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 1 parameter of function chr.",
		},

		///
		{
			s: `SELECT encode(field, "base64") FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "encode", Expr: &Call{Name: "encode", Args: []Expr{&FieldRef{Name: "field"}, &StringLiteral{Val: "base64"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT encode(field, true) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 2 parameter of function encode.",
		},

		///
		{
			s: `SELECT trunc(field, 3) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "trunc", Expr: &Call{Name: "trunc", Args: []Expr{&FieldRef{Name: "field"}, &IntegerLiteral{Val: 3}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT trunc(5, ss) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for 2 parameter of function trunc.",
		},

		///
		{
			s: `SELECT sha512(field) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "sha512", Expr: &Call{Name: "sha512", Args: []Expr{&FieldRef{Name: "field"}}}}},
				Sources: []Source{&Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT sha512(20) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 1 parameter of function sha512.",
		},

		{
			s:    `SELECT mqtt("topic") FROM tbl`,
			stmt: nil,
			err:  "Expect meta reference type for 1 parameter of function mqtt.",
		},

		{
			s:    `SELECT mqtt(topic1) FROM tbl`,
			stmt: nil,
			err:  "Parameter of mqtt function can be only topic or messageid.",
		},

		{
			s:    `SELECT split_value(topic1) FROM tbl`,
			stmt: nil,
			err:  "the arguments for split_value should be 3",
		},

		{
			s:    `SELECT split_value(topic1, 3, 1) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for 2 parameter of function split_value.",
		},
		{
			s:    `SELECT split_value(topic1, "hello", -1) FROM tbl`,
			stmt: nil,
			err:  "The index should not be a nagtive integer.",
		},
		{
			s:    `SELECT meta(tbl, "timestamp", 1) FROM tbl`,
			stmt: nil,
			err:  "The arguments for meta should be 1.",
		},
		{
			s:    `SELECT meta("src1.device") FROM tbl`,
			stmt: nil,
			err:  "Expect meta reference type for 1 parameter of function meta.",
		},
		{
			s:    `SELECT meta(device) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "meta", Expr: &Call{Name: "meta", Args: []Expr{&MetaRef{Name: "device"}}}}}, Sources: []Source{&Table{Name: "tbl"}}},
		},
		{
			s:    `SELECT meta(tbl.device) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "meta", Expr: &Call{Name: "meta", Args: []Expr{&MetaRef{StreamName: "tbl", Name: "device"}}}}}, Sources: []Source{&Table{Name: "tbl"}}},
		},
		{
			s: `SELECT meta(device->reading->topic) FROM tbl`,
			stmt: &SelectStatement{Fields: []Field{{AName: "", Name: "meta", Expr: &Call{Name: "meta", Args: []Expr{&BinaryExpr{
				OP: ARROW,
				LHS: &BinaryExpr{
					OP:  ARROW,
					LHS: &MetaRef{Name: "device"},
					RHS: &MetaRef{Name: "reading"},
				},
				RHS: &MetaRef{Name: "topic"},
			}}}}}, Sources: []Source{&Table{Name: "tbl"}}},
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
