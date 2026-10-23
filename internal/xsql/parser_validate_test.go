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

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
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

func TestParser_ParserWindowFunctionStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "select row_number() from demo ",
			err: "",
		},
		{
			s:   "select * from demo where row_number() > 1",
			err: "window functions can only be in select fields",
		},
		{
			s:   "select * from demo having row_number() > 1",
			err: "window functions can only be in select fields",
		},
		{
			s:   "select * from demo group by  row_number()",
			err: "window functions can only be in select fields",
		},
		{
			s:   "select * from demo left join demo on row_number()",
			err: "window functions can only be in select fields",
		},
		{
			s:   "select * from demo order by row_number()",
			err: "window functions can only be in select fields",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorWildcardStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT name FROM s1 UNION SELECT name FROM s2",
			err: "",
		},
		{
			s:   "SELECT * FROM s1 UNION SELECT name FROM s2",
			err: "wildcard expressions are not allowed inside set operators",
		},
		{
			s:   "SELECT name FROM s1 UNION ALL SELECT * FROM s2",
			err: "wildcard expressions are not allowed inside set operators",
		},
		{
			s:   "SELECT * FROM s1 INTERSECT SELECT * FROM s2",
			err: "wildcard expressions are not allowed inside set operators",
		},
		{
			s:   "SELECT * FROM s1 EXCEPT SELECT name FROM s2;",
			err: "wildcard expressions are not allowed inside set operators",
		},
		{
			s:   "SELECT s1.* FROM s1 UNION SELECT name FROM s2",
			err: "wildcard expressions are not allowed inside set operators",
		},
		{
			s:   "SELECT name FROM s1 UNION SELECT s2.* FROM s2",
			err: "wildcard expressions are not allowed inside set operators",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorColumnCountStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT name, age FROM s1 UNION SELECT name, age FROM s2",
			err: "",
		},
		{
			s:   "SELECT name FROM s1 UNION SELECT name, age FROM s2",
			err: "column count mismatch between left and right statement, left has 1 but right has 2",
		},
		{
			s:   "SELECT name, age FROM s1 INTERSECT ALL SELECT name FROM s2",
			err: "column count mismatch between left and right statement, left has 2 but right has 1",
		},
		{
			s:   "SELECT name, age, id FROM s1 EXCEPT SELECT name FROM s2;",
			err: "column count mismatch between left and right statement, left has 3 but right has 1",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorLimitStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT name FROM s1 LIMIT 10",
			err: "",
		},
		{
			s:   "SELECT name FROM s1 LIMIT 10 UNION SELECT name FROM s2",
			err: "LIMIT clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 UNION ALL SELECT name FROM s2 LIMIT 10",
			err: "LIMIT clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 LIMIT 5 INTERSECT SELECT name FROM s2 LIMIT 10;",
			err: "LIMIT clause is not supported inside set operators",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorOrderByStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT name FROM s1 ORDER BY name",
			err: "",
		},
		{
			s:   "SELECT name FROM s1 ORDER BY name UNION SELECT name FROM s2",
			err: "ORDER BY clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 EXCEPT ALL SELECT name FROM s2 ORDER BY name DESC",
			err: "ORDER BY clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 ORDER BY name INTERSECT SELECT name FROM s2 ORDER BY name;",
			err: "ORDER BY clause is not supported inside set operators",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserMultipleSetOperatorsStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT name FROM s1 UNION SELECT name FROM s2",
			err: "",
		},
		{
			s:   "SELECT name FROM s1 UNION SELECT name FROM s2 UNION SELECT name FROM s3",
			err: "multiple set operators are not supported: a query can combine only two SELECT statements",
		},
		{
			s:   "SELECT name FROM s1 UNION ALL SELECT name FROM s2 EXCEPT SELECT name FROM s3 INTERSECT SELECT name FROM s4",
			err: "multiple set operators are not supported: a query can combine only two SELECT statements",
		},
		{
			s:   "SELECT name FROM s1 INTERSECT SELECT name FROM s2 EXCEPT ALL SELECT name FROM s3;",
			err: "multiple set operators are not supported: a query can combine only two SELECT statements",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}

	stmts, err := NewParser(strings.NewReader("SELECT name FROM s1 UNION SELECT name FROM s2;\nSELECT name FROM s3 EXCEPT SELECT name FROM s4")).ParseQueries()
	require.NoError(t, err)
	require.Len(t, stmts, 2)
}

func TestParser_ParserSetOperatorGroupByStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT name FROM s1 GROUP BY TUMBLINGWINDOW(ss, 10)",
			err: "",
		},
		{
			s:   "SELECT name FROM s1 GROUP BY TUMBLINGWINDOW(ss, 10) UNION SELECT name FROM s2",
			err: "GROUP BY clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 INTERSECT SELECT name FROM s2 GROUP BY name",
			err: "GROUP BY clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 GROUP BY name, COUNTWINDOW(5) EXCEPT ALL SELECT name FROM s2 GROUP BY HOPPINGWINDOW(ss, 10, 5);",
			err: "GROUP BY clause is not supported inside set operators",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorJoinStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT s1.name FROM s1 LEFT JOIN t1 ON s1.id = t1.id",
			err: "",
		},
		{
			s:   "SELECT s1.name FROM s1 LEFT JOIN t1 ON s1.id = t1.id UNION SELECT name FROM s2",
			err: "JOIN clause is not supported inside set operators",
		},
		{
			s:   "SELECT name FROM s1 UNION ALL SELECT s2.name FROM s2 INNER JOIN t1 ON s2.id = t1.id",
			err: "JOIN clause is not supported inside set operators",
		},
		{
			s:   "SELECT s1.name FROM s1 RIGHT JOIN t1 ON s1.id = t1.id EXCEPT SELECT s2.name FROM s2 FULL JOIN t2 ON s2.id = t2.id;",
			err: "JOIN clause is not supported inside set operators",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorFunctionStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   `SELECT count(*) FROM s1 UNION SELECT name FROM s2`,
			err: "",
		},
		{
			s:   `SELECT name FROM s1 UNION SELECT count(*) AS c FROM s2`,
			err: "",
		},
		{
			s:   `SELECT count(name), abs(age), lag(name) FROM s1 INTERSECT SELECT name, age, id FROM s2`,
			err: "",
		},
		{
			s:   `SELECT name FROM s1 HAVING age > 18 UNION SELECT name FROM s2`,
			err: "",
		},
		{
			s:   `SELECT changed_cols("c_", true, name, age) FROM s1 UNION SELECT name FROM s2`,
			err: "function changed_cols is not supported inside set operators: it changes the number of output columns or rows",
		},
		{
			s:   `SELECT name, age FROM s1 UNION SELECT changed_cols("c_", true, name, age) FROM s2`,
			err: "function changed_cols is not supported inside set operators: it changes the number of output columns or rows",
		},
		{
			s:   `SELECT unnest(arr) FROM s1 UNION SELECT name FROM s2`,
			err: "function unnest is not supported inside set operators: it changes the number of output columns or rows",
		},
		{
			s:   `SELECT name FROM s1 UNION ALL SELECT unnest(arr) AS u FROM s2`,
			err: "function unnest is not supported inside set operators: it changes the number of output columns or rows",
		},
		{
			s:   `SELECT name FROM s1 EXCEPT SELECT unnest(arr) FROM s2;`,
			err: "function unnest is not supported inside set operators: it changes the number of output columns or rows",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorSameStreamStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT device FROM roomA UNION SELECT device FROM roomB",
			err: "",
		},
		{
			s:   "SELECT device FROM roomA WHERE temperature > 20 UNION SELECT device FROM roomA WHERE temperature < 10",
			err: "the same stream on both sides of a set operator is not supported yet",
		},
		{
			s:   "SELECT device FROM roomA WHERE temperature > 20 UNION ALL SELECT device FROM roomA WHERE temperature < 10",
			err: "the same stream on both sides of a set operator is not supported yet",
		},
		{
			s:   "SELECT device FROM roomA WHERE temperature > 20 INTERSECT SELECT device FROM roomA WHERE temperature > 25",
			err: "the same stream on both sides of a set operator is not supported yet",
		},
		{
			s:   "SELECT device FROM roomA WHERE temperature > 20 EXCEPT ALL SELECT device FROM roomA WHERE temperature > 25;",
			err: "the same stream on both sides of a set operator is not supported yet",
		},
		{
			s:   "SELECT l.device FROM roomA AS l INTERSECT ALL SELECT r.device FROM roomA AS r",
			err: "the same stream on both sides of a set operator is not supported yet",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}

func TestParser_ParserSetOperatorInvisibleStatement(t *testing.T) {
	tests := []struct {
		s   string
		err string
	}{
		{
			s:   "SELECT device, ts INVISIBLE FROM roomA UNION SELECT device FROM roomB",
			err: "",
		},
		{
			s:   "SELECT device, ts INVISIBLE, temperature FROM roomA INTERSECT SELECT device, temperature FROM roomB",
			err: "",
		},
		{
			s:   "SELECT device, temperature INVISIBLE FROM roomA UNION SELECT device, temperature FROM roomB",
			err: "column count mismatch between left and right statement, left has 1 but right has 2",
		},
		{
			s:   "SELECT device, temperature FROM roomA EXCEPT SELECT device, temperature AS temp INVISIBLE FROM roomB;",
			err: "column count mismatch between left and right statement, left has 2 but right has 1",
		},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.s)).Parse()
		if len(tt.err) == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.err, err.Error())
		}
	}
}
