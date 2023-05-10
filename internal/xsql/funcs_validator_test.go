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

package xsql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

// Ensure the parser can parse strings into Statement ASTs.
func TestFuncValidator(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *ast.SelectStatement
		err  string
	}{
		{
			s: `SELECT abs(1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "abs", Expr: &ast.Call{Name: "abs", Args: []ast.Expr{&ast.IntegerLiteral{Val: 1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT abs(field1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "abs", Expr: &ast.Call{Name: "abs", Args: []ast.Expr{&ast.FieldRef{Name: "field1", StreamName: ast.DefaultStream}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT abs(1,2) FROM tbl`,
			stmt: nil,
			err:  "Expect 1 arguments but found 2.",
		},

		{
			s: `SELECT abs(1.1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "abs", Expr: &ast.Call{Name: "abs", Args: []ast.Expr{&ast.NumberLiteral{Val: 1.1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT abs(true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT abs("test") FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT abs(ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		///
		{
			s: `SELECT sin(1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "sin", Expr: &ast.Call{Name: "sin", Args: []ast.Expr{&ast.IntegerLiteral{Val: 1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT sin(1.1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "sin", Expr: &ast.Call{Name: "sin", Args: []ast.Expr{&ast.NumberLiteral{Val: 1.1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT sin(true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT sin("test") FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT sin(ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},
		///
		{
			s: `SELECT tanh(1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "tanh", Expr: &ast.Call{Name: "tanh", Args: []ast.Expr{&ast.IntegerLiteral{Val: 1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s: `SELECT tanh(1.1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "tanh", Expr: &ast.Call{Name: "tanh", Args: []ast.Expr{&ast.NumberLiteral{Val: 1.1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT tanh(true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT tanh("test") FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT tanh(ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		///
		{
			s: `SELECT bitxor(1, 2) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "bitxor", Expr: &ast.Call{Name: "bitxor", Args: []ast.Expr{&ast.IntegerLiteral{Val: 1}, &ast.IntegerLiteral{Val: 2}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT bitxor(1.1, 2) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 1",
		},

		{
			s:    `SELECT bitxor(true, 2) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 1",
		},

		{
			s:    `SELECT bitxor(1, ss) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 2",
		},

		{
			s:    `SELECT bitxor(1, 2.2) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 2",
		},

		///
		{
			s: `SELECT bitnot(1) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "bitnot", Expr: &ast.Call{Name: "bitnot", Args: []ast.Expr{&ast.IntegerLiteral{Val: 1}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT bitnot(1.1) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 1",
		},

		{
			s:    `SELECT bitnot(true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 1",
		},

		///
		{
			s: `SELECT mod(1, 2) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "mod", Expr: &ast.Call{Name: "mod", Args: []ast.Expr{&ast.IntegerLiteral{Val: 1}, &ast.IntegerLiteral{Val: 2}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT mod("1.1", 2) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 1",
		},

		{
			s:    `SELECT mod(1.1, true) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 2",
		},

		{
			s:    `SELECT mod(1, ss) FROM tbl`,
			stmt: nil,
			err:  "Expect number - float or int type for parameter 2",
		},

		///
		{
			s: `SELECT concat(field, "hello") FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "concat", Expr: &ast.Call{Name: "concat", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.StringLiteral{Val: "hello"}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT concat("1.1", 2) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 2",
		},

		{
			s:    `SELECT concat("1.1", true) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 2",
		},

		{
			s:    `SELECT concat("1", ss) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 2",
		},

		///
		{
			s: `SELECT regexp_matches(field, "hello") FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "regexp_matches", Expr: &ast.Call{Name: "regexp_matches", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.StringLiteral{Val: "hello"}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT regexp_matches(1, "true") FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 1",
		},

		{
			s:    `SELECT regexp_matches("1.1", 2) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 2",
		},

		///
		{
			s: `SELECT regexp_replace(field, "hello", "h") FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "regexp_replace", Expr: &ast.Call{Name: "regexp_replace", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.StringLiteral{Val: "hello"}, &ast.StringLiteral{Val: "h"}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT regexp_replace(field1, "true", true) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 3",
		},

		///
		{
			s: `SELECT trim(field) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "trim", Expr: &ast.Call{Name: "trim", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT trim(1) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 1",
		},

		///
		{
			s: `SELECT rpad(field, 3) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "rpad", Expr: &ast.Call{Name: "rpad", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.IntegerLiteral{Val: 3}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT rpad("ff", true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 2",
		},

		///
		{
			s: `SELECT substring(field, 3, 4) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "substring", Expr: &ast.Call{Name: "substring", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.IntegerLiteral{Val: 3}, &ast.IntegerLiteral{Val: 4}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
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
			err:  "Expect int type for parameter 3",
		},

		///
		{
			s: `SELECT cast(field, "bigint") FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "cast", Expr: &ast.Call{Name: "cast", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.StringLiteral{Val: "bigint"}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
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
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "chr", Expr: &ast.Call{Name: "chr", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT chr(true) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 1",
		},

		///
		{
			s: `SELECT encode(field, "base64") FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "encode", Expr: &ast.Call{Name: "encode", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.StringLiteral{Val: "base64"}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT encode(field, true) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 2",
		},

		///
		{
			s: `SELECT trunc(field, 3) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "trunc", Expr: &ast.Call{Name: "trunc", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}, &ast.IntegerLiteral{Val: 3}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT trunc(5, ss) FROM tbl`,
			stmt: nil,
			err:  "Expect int type for parameter 2",
		},

		///
		{
			s: `SELECT sha512(field) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "sha512", Expr: &ast.Call{Name: "sha512", Args: []ast.Expr{&ast.FieldRef{Name: "field", StreamName: ast.DefaultStream}}}}},
				Sources: []ast.Source{&ast.Table{Name: "tbl"}},
			},
		},

		{
			s:    `SELECT sha512(20) FROM tbl`,
			stmt: nil,
			err:  "Expect string type for parameter 1",
		},

		{
			s:    `SELECT mqtt("topic") FROM tbl`,
			stmt: nil,
			err:  "Expect meta reference type for parameter 1",
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
			err:  "Expect string type for parameter 2",
		},
		{
			s:    `SELECT split_value(topic1, "hello", -1) FROM tbl`,
			stmt: nil,
			err:  "The index should not be a nagtive integer.",
		},
		{
			s:    `SELECT meta(tbl, "timestamp", 1) FROM tbl`,
			stmt: nil,
			err:  "Expect 1 arguments but found 3.",
		},
		{
			s:    `SELECT meta("src1.device") FROM tbl`,
			stmt: nil,
			err:  "Expect meta reference type for parameter 1",
		},
		{
			s:    `SELECT meta(device) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "meta", Expr: &ast.Call{Name: "meta", Args: []ast.Expr{&ast.MetaRef{Name: "device", StreamName: ast.DefaultStream}}}}}, Sources: []ast.Source{&ast.Table{Name: "tbl"}}},
		},
		{
			s:    `SELECT meta(tbl.device) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "meta", Expr: &ast.Call{Name: "meta", Args: []ast.Expr{&ast.MetaRef{StreamName: "tbl", Name: "device"}}}}}, Sources: []ast.Source{&ast.Table{Name: "tbl"}}},
		},
		{
			s: `SELECT meta(device->reading->topic) FROM tbl`,
			stmt: &ast.SelectStatement{Fields: []ast.Field{{AName: "", Name: "meta", Expr: &ast.Call{Name: "meta", Args: []ast.Expr{&ast.BinaryExpr{
				OP: ast.ARROW,
				LHS: &ast.BinaryExpr{
					OP:  ast.ARROW,
					LHS: &ast.MetaRef{Name: "device", StreamName: ast.DefaultStream},
					RHS: &ast.JsonFieldRef{Name: "reading"},
				},
				RHS: &ast.JsonFieldRef{Name: "topic"},
			}}}}}, Sources: []ast.Source{&ast.Table{Name: "tbl"}}},
		},
		{
			s: `SELECT json_path_query(data, 44) AS data
    FROM characters;`,
			stmt: nil,
			err:  "Expect string type for parameter 2",
		},
		{
			s:    `SELECT collect() from tbl`,
			stmt: nil,
			err:  "Expect 1 arguments but found 0.",
		},
		{
			s:    `SELECT deduplicate(abc, temp, true) from tbl`,
			stmt: nil,
			err:  "Expect 2 arguments but found 3.",
		},
		{
			s:    `SELECT deduplicate(temp, "string") from tbl`,
			stmt: nil,
			err:  "Expect bool type for parameter 2",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}
