// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"strings"
	"testing"
)

func TestParser_ParseTree(t *testing.T) {
	var tests = []struct {
		s    string
		stmt ast.Statement
		err  string
	}{
		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", SHARED="true");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
					KEY:        "USERID",
					SHARED:     true,
				},
			},
		},
		{
			s: `CREATE TABLE demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", RETAIN_SIZE="3");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{
					DATASOURCE:  "users",
					FORMAT:      "JSON",
					KEY:         "USERID",
					RETAIN_SIZE: 3,
				},
				StreamType: ast.TypeTable,
			},
		},
		{
			s: `CREATE TABLE table1 (
					name STRING,
					size BIGINT,
					id BIGINT
				) WITH (DATASOURCE="lookup.json", FORMAT="json", CONF_KEY="test");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("table1"),
				StreamFields: []ast.StreamField{
					{Name: "name", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "size", FieldType: &ast.BasicType{Type: ast.BIGINT}},
					{Name: "id", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{
					DATASOURCE: "lookup.json",
					FORMAT:     "json",
					CONF_KEY:   "test",
				},
				StreamType: ast.TypeTable,
			},
		},
		{
			s:    `SHOW STREAMS`,
			stmt: &ast.ShowStreamsStatement{},
		},
		{
			s:    `SHOW TABLES`,
			stmt: &ast.ShowTablesStatement{},
		},

		{
			s:    `SHOW STREAMSf`,
			stmt: nil,
			err:  `found "STREAMSf", expected keyword streams or tables.`,
		},

		{
			s:    `SHOW STREAMS d`,
			stmt: nil,
			err:  `found "d", expected semecolon or EOF.`,
		},

		{
			s: `DESCRIBE STREAM demo`,
			stmt: &ast.DescribeStreamStatement{
				Name: "demo",
			},
			err: ``,
		},

		{
			s: `EXPLAIN STREAM demo1`,
			stmt: &ast.ExplainStreamStatement{
				Name: "demo1",
			},
			err: ``,
		},

		{
			s: `DROP STREAM demo1`,
			stmt: &ast.DropStreamStatement{
				Name: "demo1",
			},
			err: ``,
		},
		{
			s: `DESCRIBE TABLE demo`,
			stmt: &ast.DescribeTableStatement{
				Name: "demo",
			},
			err: ``,
		},

		{
			s: `EXPLAIN TABLE demo1`,
			stmt: &ast.ExplainTableStatement{
				Name: "demo1",
			},
			err: ``,
		},

		{
			s: `DROP TABLE demo1`,
			stmt: &ast.DropTableStatement{
				Name: "demo1",
			},
			err: ``,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		p := NewParser(strings.NewReader(tt.s))
		stmt, err := Language.Parse(p)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}

}
