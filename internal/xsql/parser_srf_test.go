// Copyright 2023 EMQ Technologies Co., Ltd.
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

func TestParser_ParserSRFStatement(t *testing.T) {
	tests := []struct {
		s    string
		stmt *ast.SelectStatement
		err  string
	}{
		{
			s:   "select a from demo order by unnest(b)",
			err: "select statement shouldn't has srf except fields",
		},
		{
			s:   "select abc from demo left join demo on unnest(demo.a) = b GROUP BY ID, TUMBLINGWINDOW(ss, 10)",
			err: "select statement shouldn't has srf except fields",
		},
		{
			s:   "select a from demo group by id having unnest(a)",
			err: "select statement shouldn't has srf except fields",
		},
		{
			s:   "select a from demo group by unnest(a)",
			err: "select statement shouldn't has srf except fields",
		},
		{
			s:   "select a,b from demo where unnest(a)",
			err: "select statement shouldn't has srf except fields",
		},
		{
			s:   "select unnest(unnest(arr)) from demo",
			err: "select clause shouldn't has nested set-returning-functions",
		},
		{
			s:   "select abs(unnest(arr)) from demo",
			err: "select clause shouldn't has nested set-returning-functions",
		},
		{
			s:   "select unnest(arr1), unnest(arr2) from demo",
			err: "select clause shouldn't has multi set-returning-functions",
		},
		{
			s: "select unnest(arr), a from demo",
			stmt: &ast.SelectStatement{
				Fields: []ast.Field{
					{
						Name: "unnest",
						Expr: &ast.Call{
							Name:     "unnest",
							FuncId:   0,
							FuncType: ast.FuncTypeSrf,
							Args: []ast.Expr{
								&ast.FieldRef{
									StreamName: ast.DefaultStream,
									Name:       "arr",
								},
							},
						},
					},
					{
						Name: "a",
						Expr: &ast.FieldRef{
							StreamName: ast.DefaultStream,
							Name:       "a",
						},
					},
				},
				Sources: []ast.Source{&ast.Table{Name: "demo"}},
			},
		},
		{
			s: "select unnest(arr) from demo",
			stmt: &ast.SelectStatement{
				Fields: []ast.Field{
					{
						Name: "unnest",
						Expr: &ast.Call{
							Name:     "unnest",
							FuncId:   0,
							FuncType: ast.FuncTypeSrf,
							Args: []ast.Expr{
								&ast.FieldRef{
									StreamName: ast.DefaultStream,
									Name:       "arr",
								},
							},
						},
					},
				},
				Sources: []ast.Source{&ast.Table{Name: "demo"}},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}
