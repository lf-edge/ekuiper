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

package operator

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"reflect"
	"strings"
	"testing"
)

func parseStmt(p *ProjectOp, fields ast.Fields) {
	p.AllWildcard = false
	p.WildcardEmitters = make(map[string]bool)
	for _, field := range fields {
		if field.AName != "" {
			p.AliasFields = append(p.AliasFields, field)
			p.AliasNames = append(p.AliasNames, field.AName)
		} else {
			switch ft := field.Expr.(type) {
			case *ast.Wildcard:
				p.AllWildcard = true
			case *ast.FieldRef:
				if ft.Name == "*" {
					p.WildcardEmitters[string(ft.StreamName)] = true
				} else {
					p.ColNames = append(p.ColNames, []string{ft.Name, string(ft.StreamName)})
				}
			default:
				p.ExprFields = append(p.ExprFields, field)
				p.ExprNames = append(p.ExprNames, field.Name)
			}
		}
	}
}

func parseResult(opResult interface{}, aggregate bool) (result []map[string]interface{}, err error) {
	switch rt := opResult.(type) {
	case xsql.TupleRow:
		result = append(result, rt.ToMap())
	case xsql.Collection:
		result = rt.ToMaps()
	default:
		err = errors.New("unexpected result type")
	}
	return
}

func TestProjectPlan_Apply1(t *testing.T) {
	var tests = []struct {
		sql    string
		data   *xsql.Tuple
		result []map[string]interface{}
	}{
		{ //0
			sql: "SELECT a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
				Metadata: xsql.Metadata{
					"id":    45,
					"other": "mock",
				},
			},
			result: []map[string]interface{}{{
				"a": "val_a",
				"__meta": xsql.Metadata{
					"id":    45,
					"other": "mock",
				},
			}},
		},
		{ //1
			sql: "SELECT b FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: []map[string]interface{}{{}},
		},
		{ //2
			sql: "SELECT ts FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a":  "val_a",
					"ts": cast.TimeFromUnixMilli(1568854573431),
				},
			},
			result: []map[string]interface{}{{
				"ts": cast.TimeFromUnixMilli(1568854573431),
			}},
		},
		//Schemaless may return a message without selecting column
		{ //3
			sql: "SELECT ts FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a":   "val_a",
					"ts2": cast.TimeFromUnixMilli(1568854573431),
				},
			},
			result: []map[string]interface{}{{}},
		},
		{ //4
			sql: "SELECT A FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: []map[string]interface{}{{
				"A": "val_a",
			}},
		},
		//5
		{
			sql: `SELECT "value" FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"kuiper_field_0": "value",
			}},
		},
		//6
		{
			sql: `SELECT 3.4 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"kuiper_field_0": 3.4,
			}},
		},
		//7
		{
			sql: `SELECT 5 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"kuiper_field_0": 5,
			}},
		},
		//8
		{
			sql: `SELECT a, "value" AS b FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: []map[string]interface{}{{
				"a": "val_a",
				"b": "value",
			}},
		},
		//9
		{
			sql: `SELECT a, "value" AS b, 3.14 as Pi, 0 as Zero FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: []map[string]interface{}{{
				"a":    "val_a",
				"b":    "value",
				"Pi":   3.14,
				"Zero": 0,
			}},
		},
		//10
		{
			sql: `SELECT a->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{"b": "hello"},
				},
			},
			result: []map[string]interface{}{{
				"ab": "hello",
			}},
		},
		//11
		{
			sql: `SELECT a->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}(nil),
				},
			},
			result: []map[string]interface{}{{}},
		},
		//12
		{
			sql: `SELECT a->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"name": "name",
				},
			},
			result: []map[string]interface{}{{}},
		},
		//13
		{
			sql: `SELECT a->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "commonstring",
				},
			},
			result: []map[string]interface{}{{}},
		},
		//14
		{
			sql: `SELECT a[0]->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []interface{}{
						map[string]interface{}{"b": "hello1"},
						map[string]interface{}{"b": "hello2"},
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": "hello1",
			}},
		},
		//15
		{
			sql: `SELECT a[0]->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []map[string]interface{}{
						{"b": "hello1"},
						{"b": "hello2"},
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": "hello1",
			}},
		},
		//16
		{
			sql: `SELECT a[2:4] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []map[string]interface{}{
						{"b": "hello1"},
						{"b": "hello2"},
						{"b": "hello3"},
						{"b": "hello4"},
						{"b": "hello5"},
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []map[string]interface{}{
					{"b": "hello3"},
					{"b": "hello4"},
				},
			}},
		},
		//17
		{
			sql: `SELECT a[2:] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []map[string]interface{}{
						{"b": "hello1"},
						{"b": "hello2"},
						{"b": "hello3"},
						{"b": "hello4"},
						{"b": "hello5"},
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []map[string]interface{}{
					{"b": "hello3"},
					{"b": "hello4"},
					{"b": "hello5"},
				},
			}},
		},
		//18
		{
			sql: `SELECT a[2:] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []interface{}{
						true, false, true, false, true, true,
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []interface{}{
					true, false, true, true,
				},
			}},
		},
		//19
		{
			sql: `SELECT a[:4] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []interface{}{
						true, false, true, false, true, true,
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []interface{}{
					true, false, true, false,
				},
			}},
		},
		//20
		{
			sql: `SELECT a[:4] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []interface{}{
						3.14, 3.141, 3.1415, 3.14159, 3.141592, 3.1415926,
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []interface{}{
					3.14, 3.141, 3.1415, 3.14159,
				},
			}},
		},
		//21
		{
			sql: `SELECT a->b[:4] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": []float64{3.14, 3.141, 3.1415, 3.14159, 3.141592, 3.1415926},
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []float64{
					3.14, 3.141, 3.1415, 3.14159,
				},
			}},
		},
		//22
		{
			sql: `SELECT a->b[0:1] AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": []float64{3.14, 3.141, 3.1415, 3.14159, 3.141592, 3.1415926},
					},
				},
			},
			result: []map[string]interface{}{{
				"ab": []float64{
					3.14,
				},
			}},
		},
		//23
		{
			sql: `SELECT a->c->d AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": map[string]interface{}{
							"d": 35.2,
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"f1": 35.2,
			}},
		},
		//24
		{
			sql: `SELECT a->c->d AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": map[string]interface{}{
							"e": 35.2,
						},
					},
				},
			},
			result: []map[string]interface{}{{}},
		},
		//25
		{
			sql: `SELECT a->c->d AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
					},
				},
			},
			result: []map[string]interface{}{{}},
		},
		//26
		//The int type is not supported yet, the json parser returns float64 for int values
		{
			sql: `SELECT a->c->d AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": map[string]interface{}{
							"d": float64(35),
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"f1": float64(35),
			}},
		},
		//27
		{
			sql: "SELECT a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{
				{},
			},
		},
		//28
		{
			sql: "SELECT * FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{
				{},
			},
		},
		//29
		{
			sql: `SELECT * FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": map[string]interface{}{
							"d": 35.2,
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"a": map[string]interface{}{
					"b": "hello",
					"c": map[string]interface{}{
						"d": 35.2,
					},
				},
			}},
		},
		//30
		{
			sql: `SELECT * FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val1",
					"b": 3.14,
				},
			},
			result: []map[string]interface{}{{
				"a": "val1",
				"b": 3.14,
			}},
		},
		//31
		{
			sql: `SELECT 3*4 AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"f1": int64(12),
			}},
		},
		//32
		{
			sql: `SELECT 4.5*2 AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"f1": float64(9),
			}},
		},
		//33
		{
			sql: "SELECT `a.b.c` FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a.b.c": "val_a",
				},
			},
			result: []map[string]interface{}{{
				"a.b.c": "val_a",
			}},
		},
		//34
		{
			sql: `SELECT CASE a WHEN 10 THEN "true" END AS b FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": int64(10),
				},
			},
			result: []map[string]interface{}{{
				"b": "true",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestProjectPlan_Apply1")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("parse sql error： %s", err)
			continue
		}
		pp := &ProjectOp{SendMeta: true, IsAggregate: xsql.IsAggStatement(stmt)}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestProjectPlan_MultiInput(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result []map[string]interface{}
	}{ //0
		{
			sql: "SELECT * FROM tbl WHERE abc*2+3 > 12 AND abc < 20",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
			result: []map[string]interface{}{{
				"abc": int64(6),
			}},
		},
		//1
		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 OR def = \"hello\"",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
					"def": "hello",
				},
			},
			result: []map[string]interface{}{{
				"abc": int64(34),
			}},
		},
		//2
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
			}, {
				"id1": 2,
			}, {
				"id1": 3,
			}},
		},
		//3
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id2": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},

			result: []map[string]interface{}{{
				"id1": 1,
			}, {}, {
				"id1": 3,
			}},
		},
		//4
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
				"f1":  "v1",
			}, {
				"id1": 2,
				"f1":  "v2",
			}, {
				"id1": 3,
				"f1":  "v1",
			}},
		},
		//5
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id2": 2, "f2": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
				"f1":  "v1",
			}, {
				"id2": 2,
				"f2":  "v2",
			}, {
				"id1": 3,
				"f1":  "v1",
			}},
		},
		//6
		{
			sql: "SELECT src1.* FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
				"f1":  "v1",
			}, {
				"id1": 2,
				"f1":  "v2",
			}, {
				"id1": 3,
				"f1":  "v1",
			}},
		},
		//7
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
			}, {
				"id1": 2,
			}, {
				"id1": 3,
			}},
		},
		//8
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id2": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
			}, {
				"id1": 2,
			}, {}},
		},
		//9
		{
			sql: "SELECT abc FROM tbl group by abc",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "tbl",
								Message: xsql.Message{
									"abc": int64(6),
									"def": "hello",
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"abc": int64(6),
			}},
		},
		//10
		{
			sql: "SELECT abc FROM tbl group by abc",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "tbl",
								Message: xsql.Message{
									"def": "hello",
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{}},
		},
		//11
		{
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							},
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
			}, {
				"id1": 2,
			}},
		},
		//12
		{
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							},
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id2": 2, "f1": "v2"},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
			}, {}},
		},
		//13
		{
			sql: "SELECT src2.id2 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id2": 2,
			}, {
				"id2": 4,
			}, {}},
		},
		//14
		{
			sql: "SELECT src1.*, f2 FROM src1 left join src2 GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
				"f1":  "v1",
				"f2":  "w2",
			}, {
				"id1": 2,
				"f1":  "v2",
				"f2":  "w3",
			}, {
				"id1": 3,
				"f1":  "v1",
			}},
		},
		//15
		{
			sql: "SELECT * FROM src1 left join src2 GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id": 1,
				"f1": "v1",
				"f2": "w2",
			}, {
				"id": 2,
				"f1": "v2",
				"f2": "w3",
			}, {
				"id": 3,
				"f1": "v1",
			}},
		},
		//16
		{
			sql: "SELECT src1.* FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							},
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": 1,
				"f1":  "v1",
			}, {
				"id1": 2,
				"f1":  "v2",
			}},
		},
		//17
		{
			sql: "SELECT src2.id2, src1.* FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id2": 2,
				"id1": 1,
				"f1":  "v1",
			}, {
				"id2": 4,
				"id1": 2,
				"f1":  "v2",
			}, {
				"id1": 3,
				"f1":  "v1",
			}},
		},
		//18
		{
			sql: "SELECT src2.id2, src1.* FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id2": 2,
				"id1": 1,
				"f1":  "v1",
			}, {
				"id2": 4,
				"id1": 2,
				"f1":  "v2",
			}, {
				"id1": 3,
				"f1":  "v1",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestProjectPlan_MultiInput")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()

		pp := &ProjectOp{SendMeta: true, IsAggregate: xsql.IsAggStatement(stmt)}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestProjectPlan_Funcs(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result []map[string]interface{}
	}{
		//0
		{
			sql: "SELECT round(a) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": 47.5,
				},
			},
			result: []map[string]interface{}{{
				"r": float64(48),
			}},
		},
		//1
		{
			sql: "SELECT round(a) as r FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53.1},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27.4},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123.7},
					},
				},
			},

			result: []map[string]interface{}{{
				"r": float64(53),
			}, {
				"r": float64(27),
			}, {
				"r": float64(123124),
			}},
		},
		//2
		{
			sql: "SELECT round(a) as r FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53.1},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27.4},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123.7},
					},
				},
			},

			result: []map[string]interface{}{{
				"r": float64(53),
			}, {
				"r": float64(27),
			}, {
				"r": float64(123124),
			}},
		},
		//3
		{
			sql: "SELECT round(a) as r FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 65.55}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 1, "b": 12}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 73.499}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 2, "b": 34}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 3, "a": 88.88}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 3, "b": 6}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"r": float64(66),
			}, {
				"r": float64(73),
			}, {
				"r": float64(89),
			}},
		},
		//4
		{
			sql: "SELECT CONCAT(test.id, test.a, test1.b) as concat FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 65.55}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 1, "b": 12}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 73.499}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 2, "b": 34}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 3, "a": 88.88}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 3, "b": 6}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"concat": "165.5512",
			}, {
				"concat": "273.49934",
			}, {
				"concat": "388.886",
			}},
		},
		//5
		{
			sql: "SELECT count(a) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": 47.5,
				},
			},
			result: []map[string]interface{}{{
				"r": 1,
			}},
		},
		//6
		{
			sql: "SELECT meta(test.device) as d FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 65.55}, Metadata: xsql.Metadata{"device": "devicea"}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 1, "b": 12}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 73.499}, Metadata: xsql.Metadata{"device": "deviceb"}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 2, "b": 34}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 3, "a": 88.88}, Metadata: xsql.Metadata{"device": "devicec"}},
							&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 3, "b": 6}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"d": "devicea",
			}, {
				"d": "deviceb",
			}, {
				"d": "devicec",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestProjectPlan_Funcs")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Error(err)
		}
		pp := &ProjectOp{SendMeta: true, IsAggregate: xsql.IsAggStatement(stmt)}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestProjectPlan_AggFuncs(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result []map[string]interface{}
	}{
		{ //0
			sql: "SELECT count(*) as c, round(a) as r, window_start() as ws, window_end() as we FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
				},
			},
			result: []map[string]interface{}{{
				"c":  2,
				"r":  float64(122),
				"ws": int64(1541152486013),
				"we": int64(1541152487013),
			}, {
				"c":  2,
				"r":  float64(89),
				"ws": int64(1541152486013),
				"we": int64(1541152487013),
			}},
		},
		//1
		{
			sql: "SELECT count(a) as c, avg(a) as a, sum(a) as s, min(a) as min, max(a) as max FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c":   1,
				"a":   122.33,
				"s":   122.33,
				"min": 122.33,
				"max": 122.33,
			}, {
				"c":   2,
				"s":   103.63,
				"a":   51.815,
				"min": 14.6,
				"max": 89.03,
			}},
		},
		//2
		{
			sql: "SELECT avg(a) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.54}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 98.31}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.54}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"avg": 116.68,
			}, {
				"avg": 51.815,
			}},
		},
		//3
		{
			sql: "SELECT max(a) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"max": 177.51,
			}, {
				"max": 89.03,
			}},
		},
		//4
		{
			sql: "SELECT min(a), window_start(), window_end() FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},

			result: []map[string]interface{}{{
				"min":          68.55,
				"window_start": int64(1541152486013),
				"window_end":   int64(1541152487013),
			}},
		},
		//5
		{
			sql: "SELECT count(*) as all, count(a) as c, avg(a) as a, sum(a) as s, min(a) as min, max(a) as max FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
			},

			result: []map[string]interface{}{{
				"all": 3,
				"c":   2,
				"a":   123.03,
				"s":   246.06,
				"min": 68.55,
				"max": 177.51,
			}},
		},
		//6
		{
			sql: "SELECT sum(a), window_start() as ws, window_end() FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: []map[string]interface{}{{
				"sum":        123203,
				"ws":         int64(1541152486013),
				"window_end": int64(1541152487013),
			}},
		},
		//7
		{
			sql: "SELECT sum(a) as s FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "s": 123203},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},
			},

			result: []map[string]interface{}{{
				"s": 123203,
			}},
		},
		//8
		{
			sql: "SELECT sum(a) FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},
			},
			result: []map[string]interface{}{{
				"sum": 123203,
			}},
		},
		//9
		{
			sql: "SELECT count(*) as all, count(a) as c, avg(a) as a, sum(a) as s, min(a) as min, max(a) as max  FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"s": 123123},
					},
				},
			},
			result: []map[string]interface{}{{
				"all": 3,
				"c":   2,
				"a":   40,
				"s":   80,
				"min": 27,
				"max": 53,
			}},
		},
		//10
		{
			sql: "SELECT count(*), meta(test1.device) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 1, "color": "w2"}, Metadata: xsql.Metadata{"device": "devicea"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 5, "color": "w2"}, Metadata: xsql.Metadata{"device": "deviceb"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 2, "color": "w1"}, Metadata: xsql.Metadata{"device": "devicec"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 4, "color": "w1"}, Metadata: xsql.Metadata{"device": "deviced"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"count": 2,
				"meta":  "devicea",
			}, {
				"count": 2,
				"meta":  "devicec",
			}},
		},
		//11
		{
			sql: "SELECT count(*) as c, meta(test1.device) as d FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "d": "devicea"}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 1, "color": "w2"}, Metadata: xsql.Metadata{"device": "devicea"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 5, "color": "w2"}, Metadata: xsql.Metadata{"device": "deviceb"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "d": "devicec"}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 2, "color": "w1"}, Metadata: xsql.Metadata{"device": "devicec"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "test1", Message: xsql.Message{"id": 4, "color": "w1"}, Metadata: xsql.Metadata{"device": "deviced"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c": 2,
				"d": "devicea",
			}, {
				"c": 2,
				"d": "devicec",
			}},
		},
		//12
		{
			sql: "SELECT * FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
				},
			},
			result: []map[string]interface{}{{
				"a":     122.33,
				"c":     2,
				"color": "w2",
				"id":    1,
				"r":     122,
			}, {
				"a":     89.03,
				"c":     2,
				"color": "w1",
				"id":    2,
				"r":     89,
			}},
		},
		//13
		{
			sql: "SELECT collect(a) as r1 FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"r1": []interface{}{122.33, 177.51},
			}, {"r1": []interface{}{89.03, 14.6}}},
		},
		//14
		{
			sql: "SELECT collect(*)[1] as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "s": 123203},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},

				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: []map[string]interface{}{{
				"c1": xsql.Message{
					"a": 27,
				},
			}},
		},
		//15
		{
			sql: "SELECT collect(*)[1]->a as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "s": 123203},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},
			},

			result: []map[string]interface{}{{
				"c1": 27,
			}},
		},
		//16
		{
			sql: "SELECT collect(*)[1]->sl[0] as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "sl": []string{"hello", "world"}},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27, "sl": []string{"new", "horizon"}},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123, "sl": []string{"south", "africa"}},
					},
				},
			},

			result: []map[string]interface{}{{
				"c1": "new",
			}},
		},
		//17
		{
			sql: "SELECT deduplicate(id, true) as r1 FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{
				{
					"r1": []interface{}{
						xsql.Message{"a": 122.33, "c": 2, "color": "w2", "id": 1, "r": 122},
						xsql.Message{"a": 177.51, "color": "w2", "id": 5}},
				}, {
					"r1": []interface{}{
						xsql.Message{"a": 89.03, "c": 2, "color": "w1", "id": 2, "r": 89},
						xsql.Message{"a": 14.6, "color": "w1", "id": 4}},
				},
			},
		},
		//18
		{
			sql: "SELECT deduplicate(a, false)->a as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "s": 123203},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},
			},

			result: []map[string]interface{}{{
				"c1": 123123,
			}},
		},
		//19
		{
			sql: "SELECT deduplicate(a, false) as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "s": 123203},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53},
					},
				},
			},

			result: []map[string]interface{}{{}},
		},
		//20
		{
			sql: "SELECT deduplicate(a, false) as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53, "s": 123203},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 27},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53},
					},
				},
			},

			result: []map[string]interface{}{{}},
		},
		//21  when got column after group by operation, return the first tuple's column
		{
			sql: "SELECT A.module, A.topic , max(A.value), B.topic as var2, max(B.value) as max2, C.topic as var3, max(C.value) as max3 FROM A FULL JOIN B on A.module=B.module FULL JOIN C on A.module=C.module GROUP BY A.module, TUMBLINGWINDOW(ss, 10)",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "B", Message: xsql.Message{"module": 1, "topic": "moduleB topic", "value": 1}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "C", Message: xsql.Message{"module": 1, "topic": "moduleC topic", "value": 100}},
								},
							},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"var2": "moduleB topic",
				"max2": 1,
				"max3": 100,
			}},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestProjectPlan_AggFuncs")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Error(err)
		}
		pp := &ProjectOp{SendMeta: true, IsAggregate: true}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestProjectPlanError(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		//0
		{
			sql:    "SELECT a FROM test",
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		},
		//1
		{
			sql: "SELECT a * 5 FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: errors.New("run Select error: invalid operation string(val_a) * int64(5)"),
		},
		//2
		{
			sql: `SELECT a[0]->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "common string",
				},
			},
			result: errors.New("run Select error: invalid operation string(common string) [] *xsql.BracketEvalResult(&{0 0})"),
		},
		//3
		{
			sql: `SELECT round(a) as r FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "common string",
				},
			},
			result: errors.New("run Select error: call func round error: only float64 & int type are supported"),
		},
		//4
		{
			sql: `SELECT round(a) as r FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"abc": "common string",
				},
			},
			result: errors.New("run Select error: call func round error: only float64 & int type are supported"),
		},
		//5
		{
			sql: "SELECT avg(a) as avg FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.54}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": "dde"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.54}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: errors.New("run Select error: call func avg error: requires float64 but found string(dde)"),
		},
		//6
		{
			sql: "SELECT sum(a) as sum FROM test GROUP BY TumblingWindow(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 53},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": "ddd"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"a": 123123},
					},
				},
			},

			result: errors.New("run Select error: call func sum error: requires int but found string(ddd)"),
		},
		//7
		{
			sql: `SELECT a[0]->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": []map[string]interface{}(nil),
				},
			},
			result: errors.New("run Select error: out of index: 0 of 0"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestProjectPlanError")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		pp := &ProjectOp{SendMeta: true, IsAggregate: xsql.IsAggStatement(stmt)}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, opResult) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, opResult)
		}
	}
}
