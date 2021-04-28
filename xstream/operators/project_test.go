package operators

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"strings"
	"testing"
)

func TestProjectPlan_Apply1(t *testing.T) {
	var tests = []struct {
		sql    string
		data   *xsql.Tuple
		result []map[string]interface{}
	}{
		{
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
				"__meta": map[string]interface{}{
					"id":    float64(45),
					"other": "mock",
				},
			}},
		},
		{
			sql: "SELECT b FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: []map[string]interface{}{{}},
		},
		{
			sql: "SELECT ts FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a":  "val_a",
					"ts": common.TimeFromUnixMilli(1568854573431),
				},
			},
			result: []map[string]interface{}{{
				"ts": "2019-09-19T00:56:13.431Z",
			}},
		},
		//Schemaless may return a message without selecting column
		{
			sql: "SELECT ts FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a":   "val_a",
					"ts2": common.TimeFromUnixMilli(1568854573431),
				},
			},
			result: []map[string]interface{}{{}},
		},
		{
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

		{
			sql: `SELECT "value" FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				DEFAULT_FIELD_NAME_PREFIX + "0": "value",
			}},
		},

		{
			sql: `SELECT 3.4 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				DEFAULT_FIELD_NAME_PREFIX + "0": 3.4,
			}},
		},

		{
			sql: `SELECT 5 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				DEFAULT_FIELD_NAME_PREFIX + "0": 5.0,
			}},
		},

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
				"Zero": 0.0,
			}},
		},

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
				"ab": []interface{}{
					map[string]interface{}{"b": "hello3"},
					map[string]interface{}{"b": "hello4"},
				},
			}},
		},

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
				"ab": []interface{}{
					map[string]interface{}{"b": "hello3"},
					map[string]interface{}{"b": "hello4"},
					map[string]interface{}{"b": "hello5"},
				},
			}},
		},

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
				"ab": []interface{}{
					3.14, 3.141, 3.1415, 3.14159,
				},
			}},
		},

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
				"ab": []interface{}{
					3.14,
				},
			}},
		},

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

		{
			sql: `SELECT 3*4 AS f1 FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"f1": float64(12),
			}},
		},

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
	contextLogger := common.Log.WithField("rule", "TestProjectPlan_Apply1")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("parse sql error： %s", err)
			continue
		}
		pp := &ProjectOp{Fields: stmt.Fields, SendMeta: true}
		pp.isTest = true
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("%d. The returned result %#v is not type of []byte\n", result, i)
		}
	}
}

func TestProjectPlan_MultiInput(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result []map[string]interface{}
	}{
		{
			sql: "SELECT * FROM tbl WHERE abc*2+3 > 12 AND abc < 20",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
			result: []map[string]interface{}{{
				"abc": float64(6), //json marshall problem
			}},
		},

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
				"abc": float64(34),
			}},
		},

		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
			}, {
				"id1": float64(2),
			}, {
				"id1": float64(3),
			}},
		},
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id2": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
			}, {}, {
				"id1": float64(3),
			}},
		},
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
				"f1":  "v1",
			}, {
				"id1": float64(2),
				"f1":  "v2",
			}, {
				"id1": float64(3),
				"f1":  "v1",
			}},
		},
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id2": 2, "f2": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
				"f1":  "v1",
			}, {
				"id2": float64(2),
				"f2":  "v2",
			}, {
				"id1": float64(3),
				"f1":  "v1",
			}},
		},
		{
			sql: "SELECT src1.* FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
				"f1":  "v1",
			}, {
				"id1": float64(2),
				"f1":  "v2",
			}, {
				"id1": float64(3),
				"f1":  "v1",
			}},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
			}, {
				"id1": float64(2),
			}, {
				"id1": float64(3),
			}},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id2": 3, "f1": "v1"}},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
			}, {
				"id1": float64(2),
			}, {}},
		},
		{
			sql: "SELECT abc FROM tbl group by abc",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"abc": int64(6),
							"def": "hello",
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"abc": float64(6),
			}},
		},
		{
			sql: "SELECT abc FROM tbl group by abc",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"def": "hello",
						},
					},
				},
			},
			result: []map[string]interface{}{{}},
		},
		{
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
			}, {
				"id1": float64(2),
			}},
		},
		{
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id2": 2, "f1": "v2"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
			}, {}},
		},
		{
			sql: "SELECT src2.id2 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id2": float64(2),
			}, {
				"id2": float64(4),
			}, {}},
		},
		{
			sql: "SELECT src1.*, f2 FROM src1 left join src2 GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
				"f1":  "v1",
				"f2":  "w2",
			}, {
				"id1": float64(2),
				"f1":  "v2",
				"f2":  "w3",
			}, {
				"id1": float64(3),
				"f1":  "v1",
			}},
		},
		{
			sql: "SELECT * FROM src1 left join src2 GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id": 3, "f1": "v1"}},
					},
				},
			},
			result: []map[string]interface{}{{
				"id": float64(1),
				"f1": "v1",
				"f2": "w2",
			}, {
				"id": float64(2),
				"f1": "v2",
				"f2": "w3",
			}, {
				"id": float64(3),
				"f1": "v1",
			}},
		},
		{
			sql: "SELECT src1.* FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": float64(1),
				"f1":  "v1",
			}, {
				"id1": float64(2),
				"f1":  "v2",
			}},
		},
		{
			sql: "SELECT src2.id2, src1.* FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id2": float64(2),
				"id1": float64(1),
				"f1":  "v1",
			}, {
				"id2": float64(4),
				"id1": float64(2),
				"f1":  "v2",
			}, {
				"id1": float64(3),
				"f1":  "v1",
			}},
		},
		{
			sql: "SELECT src2.id2, src1.* FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id2": float64(2),
				"id1": float64(1),
				"f1":  "v1",
			}, {
				"id2": float64(4),
				"id1": float64(2),
				"f1":  "v2",
			}, {
				"id1": float64(3),
				"f1":  "v1",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestProjectPlan_MultiInput")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()

		pp := &ProjectOp{Fields: stmt.Fields}
		pp.isTest = true
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}

			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}

func TestProjectPlan_Funcs(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result []map[string]interface{}
	}{
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
		}, {
			sql: "SELECT round(a) as r FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53.1},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27.4},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123.7},
						},
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
		}, {
			sql: "SELECT round(a) as r FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53.1},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27.4},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123.7},
						},
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
		}, {
			sql: "SELECT round(a) as r FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 65.55}},
						{Emitter: "test1", Message: xsql.Message{"id": 1, "b": 12}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 2, "a": 73.499}},
						{Emitter: "test1", Message: xsql.Message{"id": 2, "b": 34}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 3, "a": 88.88}},
						{Emitter: "test1", Message: xsql.Message{"id": 3, "b": 6}},
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
		}, {
			sql: "SELECT CONCAT(test.id, test.a, test1.b) as concat FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 65.55}},
						{Emitter: "test1", Message: xsql.Message{"id": 1, "b": 12}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 2, "a": 73.499}},
						{Emitter: "test1", Message: xsql.Message{"id": 2, "b": 34}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 3, "a": 88.88}},
						{Emitter: "test1", Message: xsql.Message{"id": 3, "b": 6}},
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
		}, {
			sql: "SELECT count(a) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": 47.5,
				},
			},
			result: []map[string]interface{}{{
				"r": float64(1),
			}},
		}, {
			sql: "SELECT meta(test.device) as d FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 65.55}, Metadata: xsql.Metadata{"device": "devicea"}},
						{Emitter: "test1", Message: xsql.Message{"id": 1, "b": 12}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 2, "a": 73.499}, Metadata: xsql.Metadata{"device": "deviceb"}},
						{Emitter: "test1", Message: xsql.Message{"id": 2, "b": 34}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 3, "a": 88.88}, Metadata: xsql.Metadata{"device": "devicec"}},
						{Emitter: "test1", Message: xsql.Message{"id": 3, "b": 6}},
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
	contextLogger := common.Log.WithField("rule", "TestProjectPlan_Funcs")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Error(err)
		}
		pp := &ProjectOp{Fields: stmt.Fields, IsAggregate: xsql.IsAggStatement(stmt)}
		pp.isTest = true
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}

			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("%d. The returned result is not type of []byte\n", i)
		}
	}
}

func TestProjectPlan_AggFuncs(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result []map[string]interface{}
	}{
		{
			sql: "SELECT count(*) as c, round(a) as r FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c": float64(2),
				"r": float64(122),
			}, {
				"c": float64(2),
				"r": float64(89),
			}},
		},
		{
			sql: "SELECT count(a) as c, avg(a) as a, sum(a) as s, min(a) as min, max(a) as max FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c":   float64(1),
				"a":   122.33,
				"s":   122.33,
				"min": 122.33,
				"max": 122.33,
			}, {
				"c":   float64(2),
				"s":   103.63,
				"a":   51.815,
				"min": 14.6,
				"max": 89.03,
			}},
		},
		{
			sql: "SELECT avg(a) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.54}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 98.31}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.54}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
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
		{
			sql: "SELECT max(a) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
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
		{
			sql: "SELECT min(a) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
						{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
					},
				},
			},

			result: []map[string]interface{}{{
				"min": 68.55,
			}},
		}, {
			sql: "SELECT count(*) as all, count(a) as c, avg(a) as a, sum(a) as s, min(a) as min, max(a) as max FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
						{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
					},
				},
			},

			result: []map[string]interface{}{{
				"all": float64(3),
				"c":   float64(2),
				"a":   123.03,
				"s":   246.06,
				"min": 68.55,
				"max": 177.51,
			}},
		}, {
			sql: "SELECT sum(a) FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"sum": float64(123203),
			}},
		}, {
			sql: "SELECT sum(a) as s FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "s": 123203},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"s": float64(123203),
			}},
		}, {
			sql: "SELECT sum(a) FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"sum": float64(123203),
			}},
		}, {
			sql: "SELECT count(*) as all, count(a) as c, avg(a) as a, sum(a) as s, min(a) as min, max(a) as max  FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"s": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"all": float64(3),
				"c":   float64(2),
				"a":   float64(40),
				"s":   float64(80),
				"min": float64(27),
				"max": float64(53),
			}},
		},
		{
			sql: "SELECT count(*), meta(test1.device) FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
							{Emitter: "test1", Message: xsql.Message{"id": 1, "color": "w2"}, Metadata: xsql.Metadata{"device": "devicea"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "test1", Message: xsql.Message{"id": 5, "color": "w2"}, Metadata: xsql.Metadata{"device": "deviceb"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
							{Emitter: "test1", Message: xsql.Message{"id": 2, "color": "w1"}, Metadata: xsql.Metadata{"device": "devicec"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "test1", Message: xsql.Message{"id": 4, "color": "w1"}, Metadata: xsql.Metadata{"device": "deviced"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"count": float64(2),
				"meta":  "devicea",
			}, {
				"count": float64(2),
				"meta":  "devicec",
			}},
		},
		{
			sql: "SELECT count(*) as c, meta(test1.device) as d FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "d": "devicea"}},
							{Emitter: "test1", Message: xsql.Message{"id": 1, "color": "w2"}, Metadata: xsql.Metadata{"device": "devicea"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "test1", Message: xsql.Message{"id": 5, "color": "w2"}, Metadata: xsql.Metadata{"device": "deviceb"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "d": "devicec"}},
							{Emitter: "test1", Message: xsql.Message{"id": 2, "color": "w1"}, Metadata: xsql.Metadata{"device": "devicec"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "test1", Message: xsql.Message{"id": 4, "color": "w1"}, Metadata: xsql.Metadata{"device": "deviced"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c": float64(2),
				"d": "devicea",
			}, {
				"c": float64(2),
				"d": "devicec",
			}},
		}, {
			sql: "SELECT * FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"a":     float64(122.33),
				"c":     float64(2),
				"color": "w2",
				"id":    float64(1),
				"r":     float64(122),
			}, {
				"a":     float64(89.03),
				"c":     float64(2),
				"color": "w1",
				"id":    float64(2),
				"r":     float64(89),
			}},
		}, {
			sql: "SELECT collect(a) as r1 FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"r1": []interface{}{122.33, 177.51},
			}, {"r1": []interface{}{89.03, 14.6}}},
		}, {
			sql: "SELECT collect(*)[1] as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "s": 123203},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c1": map[string]interface{}{
					"a": float64(27),
				},
			}},
		}, {
			sql: "SELECT collect(*)[1]->a as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "s": 123203},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c1": float64(27),
			}},
		}, {
			sql: "SELECT collect(*)[1]->sl[0] as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "sl": []string{"hello", "world"}},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27, "sl": []string{"new", "horizon"}},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123, "sl": []string{"south", "africa"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c1": "new",
			}},
		}, {
			sql: "SELECT deduplicate(id, true) as r1 FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2, "r": 122}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 2, "r": 89}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
						},
					},
				},
			},
			result: []map[string]interface{}{
				{
					"r1": []interface{}{
						map[string]interface{}{"a": float64(122.33), "c": float64(2), "color": "w2", "id": float64(1), "r": float64(122)},
						map[string]interface{}{"a": float64(177.51), "color": "w2", "id": float64(5)}},
				}, {
					"r1": []interface{}{
						map[string]interface{}{"a": float64(89.03), "c": float64(2), "color": "w1", "id": float64(2), "r": float64(89)},
						map[string]interface{}{"a": float64(14.6), "color": "w1", "id": float64(4)}},
				},
			},
		}, {
			sql: "SELECT deduplicate(a, false)->a as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "s": 123203},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"c1": float64(123123),
			}},
		}, {
			sql: "SELECT deduplicate(a, false) as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "s": 123203},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 53},
						},
					},
				},
			},
			result: []map[string]interface{}{{}},
		}, {
			sql: "SELECT deduplicate(a, false) as c1 FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53, "s": 123203},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 27},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 53},
						},
					},
				},
			},
			result: []map[string]interface{}{{}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestProjectPlan_AggFuncs")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Error(err)
		}
		pp := &ProjectOp{Fields: stmt.Fields, IsAggregate: true, isTest: true}
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}

			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("%d. %q\n\nThe returned result is not type of []byte: %#v\n", i, tt.sql, result)
		}
	}
}

func TestProjectPlanError(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql:    "SELECT a FROM test",
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		}, {
			sql: "SELECT a * 5 FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "val_a",
				},
			},
			result: errors.New("run Select error: invalid operation string(val_a) * int64(5)"),
		}, {
			sql: `SELECT a[0]->b AS ab FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "common string",
				},
			},
			result: errors.New("run Select error: invalid operation string(common string) [] *xsql.BracketEvalResult(&{0 0})"),
		}, {
			sql: `SELECT round(a) as r FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "common string",
				},
			},
			result: errors.New("run Select error: call func round error: only float64 & int type are supported"),
		}, {
			sql: `SELECT round(a) as r FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"abc": "common string",
				},
			},
			result: errors.New("run Select error: call func round error: only float64 & int type are supported"),
		}, {
			sql: "SELECT avg(a) as avg FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
			data: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.54}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": "dde"}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.54}},
							{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03}},
							{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "test", Message: xsql.Message{"id": 4, "a": 14.6}},
							{Emitter: "src2", Message: xsql.Message{"id": 4, "color": "w1"}},
						},
					},
				},
			},
			result: errors.New("run Select error: call func avg error: requires float64 but found string(dde)"),
		}, {
			sql: "SELECT sum(a) as sum FROM test GROUP BY TumblingWindow(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "test",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"a": 53},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": "ddd"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"a": 123123},
						},
					},
				},
			},
			result: errors.New("run Select error: call func sum error: requires int but found string(ddd)"),
		}, {
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
	contextLogger := common.Log.WithField("rule", "TestProjectPlanError")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()

		pp := &ProjectOp{Fields: stmt.Fields, IsAggregate: xsql.IsAggStatement(stmt)}
		pp.isTest = true
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
