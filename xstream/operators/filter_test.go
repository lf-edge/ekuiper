package operators

import (
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"strings"
	"testing"
)

func TestFilterPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 AND abc < 20",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
			},
			result: nil,
		},
		// nil equals nil?
		{
			sql: "SELECT a FROM tbl WHERE def = ghi",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
			},
		},
		{
			sql: "SELECT * FROM tbl WHERE abc > def and abc <= ghi",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": common.TimeFromUnixMilli(1568854515000),
					"def": common.TimeFromUnixMilli(1568853515000),
					"ghi": common.TimeFromUnixMilli(1568854515000),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": common.TimeFromUnixMilli(1568854515000),
					"def": common.TimeFromUnixMilli(1568853515000),
					"ghi": common.TimeFromUnixMilli(1568854515000),
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 AND abc < 20",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
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
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
					"def": "hello",
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE abc > \"2019-09-19T00:55:15.000Z\"",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": common.TimeFromUnixMilli(1568854515678),
					"def": "hello",
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": common.TimeFromUnixMilli(1568854515678),
					"def": "hello",
				},
			},
		},

		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
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
			result: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
		},

		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v8\" GROUP BY TUMBLINGWINDOW(ss, 10)",
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
			result: nil,
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
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v22\" GROUP BY TUMBLINGWINDOW(ss, 10)",
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
			result: nil,
		},
		{
			sql: "SELECT abc FROM tbl WHERE meta(topic) = \"topic1\" ",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
				Metadata: xsql.Metadata{
					"topic": "topic1",
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
				Metadata: xsql.Metadata{
					"topic": "topic1",
				},
			},
		},
		{
			sql: `SELECT abc FROM tbl WHERE json_path_exists(samplers, "$[? @.result.throughput==30]")`,
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"samplers": []interface{}{
						map[string]interface{}{
							"name": "page1",
							"result": map[string]interface{}{
								"throughput": float64(25),
								"rt":         float64(20),
							},
						},
						map[string]interface{}{
							"name": "page2",
							"result": map[string]interface{}{
								"throughput": float64(30),
								"rt":         float64(20),
							},
						},
					},
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"samplers": []interface{}{
						map[string]interface{}{
							"name": "page1",
							"result": map[string]interface{}{
								"throughput": float64(25),
								"rt":         float64(20),
							},
						},
						map[string]interface{}{
							"name": "page2",
							"result": map[string]interface{}{
								"throughput": float64(30),
								"rt":         float64(20),
							},
						},
					},
				},
			},
		},
		{
			sql: `SELECT abc FROM tbl WHERE json_path_exists(samplers, "$[? @.result.throughput<20]")`,
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"samplers": []interface{}{
						map[string]interface{}{
							"name": "page1",
							"result": map[string]interface{}{
								"throughput": 25,
								"rt":         20,
							},
						},
						map[string]interface{}{
							"name": "page2",
							"result": map[string]interface{}{
								"throughput": 30,
								"rt":         20,
							},
						},
					},
				},
			},
			result: nil,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestAggregatePlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		pp := &FilterOp{Condition: stmt.Condition}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestFilterPlanError(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "SELECT a FROM tbl WHERE a = b",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
					"b": "astring",
				},
			},
			result: errors.New("run Where error: invalid operation int64(6) = string(astring)"),
		},
		{
			sql:    "SELECT a FROM tbl WHERE def = ghi",
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		},
		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
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
				xsql.WindowTuples{
					Emitter: "src2",
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
			result: errors.New("run Where error: the input WindowTuplesSet with multiple tuples cannot be evaluated"),
		},

		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v8\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": 3},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: errors.New("run Where error: invalid operation int64(3) = string(v8)"),
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": 50}},
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
			result: errors.New("run Where error: invalid operation int64(50) = string(v1)"),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestAggregatePlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		pp := &FilterOp{Condition: stmt.Condition}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
