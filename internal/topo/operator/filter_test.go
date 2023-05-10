// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestFilterPlan_Apply(t *testing.T) {
	tests := []struct {
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
					"abc": cast.TimeFromUnixMilli(1568854515000),
					"def": cast.TimeFromUnixMilli(1568853515000),
					"ghi": cast.TimeFromUnixMilli(1568854515000),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": cast.TimeFromUnixMilli(1568854515000),
					"def": cast.TimeFromUnixMilli(1568853515000),
					"ghi": cast.TimeFromUnixMilli(1568854515000),
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
					"abc": cast.TimeFromUnixMilli(1568854515678),
					"def": "hello",
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": cast.TimeFromUnixMilli(1568854515678),
					"def": "hello",
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE def IN (\"hello\") AND abc IN (34, 33)",
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
			sql: "SELECT abc FROM tbl WHERE def NOT IN (\"ello\") AND abc NOT IN (35, 33)",
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
			sql: "SELECT abc FROM tbl WHERE def IN strArraySet AND abc IN intArraySet",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": []string{"hello", "world"},
					"intArraySet": []int{33, 34},
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": []string{"hello", "world"},
					"intArraySet": []int{33, 34},
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE def NOT IN strArraySet AND abc NOT IN intArraySet",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": []string{"ello", "world"},
					"intArraySet": []int{33, 35},
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": []string{"ello", "world"},
					"intArraySet": []int{33, 35},
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE def IN (\"ello\")",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
				},
			},
			result: nil,
		},

		{
			sql: "SELECT abc FROM tbl WHERE def NOT IN (\"ello\")",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
				},
			},
			result: nil,
		},

		{
			sql: "SELECT abc FROM tbl WHERE def IN strArraySet",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": nil,
				},
			},
			result: nil,
		},

		{
			sql: "SELECT abc FROM tbl WHERE def NOT IN strArraySet",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": nil,
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc":         int64(34),
					"def":         "hello",
					"strArraySet": nil,
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE abc IN (abc, def, ghm)",
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
			sql: "SELECT abc FROM tbl WHERE abc NOT IN (def, ghm)",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
					"def": int64(35),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
					"def": int64(35),
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE json->abc IN json->intArraySet",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"json": map[string]interface{}{
						"abc":         int64(34),
						"def":         "hello",
						"strArraySet": []string{"hello", "world"},
						"intArraySet": []int{33, 34},
					},
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"json": map[string]interface{}{
						"abc":         int64(34),
						"def":         "hello",
						"strArraySet": []string{"hello", "world"},
						"intArraySet": []int{33, 34},
					},
				},
			},
		},

		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
		},
		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v8\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{},
			},
		},
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
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 IN (\"v1\") GROUP BY TUMBLINGWINDOW(ss, 10)",
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
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v22\" GROUP BY TUMBLINGWINDOW(ss, 10)",
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
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{},
			},
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
		// Test for short circuit. If not, an error would happen
		{
			sql: "SELECT abc FROM tbl WHERE abc*2 > 12 AND abc / 0 < 20",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
			result: nil,
		},
		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 OR abc / 0 < 20",
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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestFilerPlan_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement %d parse error %s", i, err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
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
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 WHERE f1 = \"v8\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": 3},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: errors.New("run Where error: invalid operation int64(3) = string(v8)"),
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": 50}},
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
			result: errors.New("run Where error: invalid operation int64(50) = string(v1)"),
		},
		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 AND abc / 0 < 20",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
			result: errors.New("run Where error: divided by zero"),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestFilterPlanError")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		pp := &FilterOp{Condition: stmt.Condition}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
