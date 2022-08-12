// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"reflect"
	"strings"
	"testing"
)

func TestHavingPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: `SELECT id1 FROM src1 HAVING avg(id1) > 1`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 5, "f1": "v1"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 5, "f1": "v1"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
		},
		{
			sql: `SELECT id1 FROM src1 HAVING sum(id1) > 1`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
			result: nil,
		},

		{
			sql: `SELECT id1 FROM src1 HAVING sum(id1) = 1`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
		},

		{
			sql: `SELECT id1 FROM src1 HAVING max(id1) > 10`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
			result: nil,
		},
		{
			sql: `SELECT id1 FROM src1 HAVING max(id1) = 1`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
		}, {
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 having f1 = \"v2\"",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							},
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486055, 1541152487055),
					},
				},
			},
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486055, 1541152487055),
					},
				},
			},
		}, {
			sql: "SELECT count(*) as c, round(a) as r FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color having a > 100",
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
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
				},
			},
			result: &xsql.GroupedTuplesSet{
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
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestHavingPlan_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		pp := &HavingOp{Condition: stmt.Having}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestHavingPlanAlias_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: `SELECT avg(id1) as a FROM src1 HAVING a > 1`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "a": 8 / 3},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 5, "f1": "v1"},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "a": 8 / 3},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 5, "f1": "v1"},
					},
				},
			},
		},
		{
			sql: `SELECT sum(id1) as s FROM src1 HAVING s > 1`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "s": 1},
					},
				},
			},
			result: nil,
		}, {
			sql: "SELECT count(*) as c FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 having c > 1",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1", "c": 2},
							},
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
							},
						},
					},
				},
			},
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1", "c": 2},
							},
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
				},
			},
		}, {
			sql: "SELECT count(*) as c, round(a) as r FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10), test1.color having c > 1",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2}},
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
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 2, "a": 89.03, "c": 1}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id": 2, "color": "w1"}},
								},
							},
						},
					},
				},
			},
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 2}},
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
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestHavingPlan_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		pp := &HavingOp{Condition: stmt.Having}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestHavingPlanError(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: `SELECT id1 FROM src1 HAVING avg(id1) > "str"`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{Emitter: "src1",
						Message: xsql.Message{"id1": 5, "f1": "v1"},
					},
				},
			},
			result: errors.New("run Having error: invalid operation int64(2) > string(str)"),
		}, {
			sql:    `SELECT id1 FROM src1 HAVING avg(id1) > "str"`,
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		}, {
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 having f1 = \"v2\"",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": 3},
							},
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": 3},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
					},
				},
			},
			result: errors.New("run Having error: invalid operation int64(3) = string(v2)"),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestHavingPlan_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		pp := &HavingOp{Condition: stmt.Having}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
