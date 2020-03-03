package plans

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
			},
			result: []map[string]interface{}{{
				"a": "val_a",
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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestProjectPlan_Apply1")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()

		pp := &ProjectPlan{Fields: stmt.Fields}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("%d. The returned result is not type of []byte\n", i)
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

		pp := &ProjectPlan{Fields: stmt.Fields}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}

			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

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
		pp := &ProjectPlan{Fields: stmt.Fields, IsAggregate: xsql.IsAggStatement(stmt)}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}

			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

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
							{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
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
				"c": float64(2),
				"r": float64(122),
			}, {
				"c": float64(2),
				"r": float64(89),
			}},
		},
		{
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
			sql: "SELECT max(a) as max FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color",
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
			sql: "SELECT min(a) as min FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
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
		pp := &ProjectPlan{Fields: stmt.Fields, IsAggregate: true}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}

			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

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
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestProjectPlanError")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()

		pp := &ProjectPlan{Fields: stmt.Fields, IsAggregate: xsql.IsAggStatement(stmt)}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
