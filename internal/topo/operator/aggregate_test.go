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
	"reflect"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestAggregatePlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result *xsql.GroupedTuplesSet
	}{
		{
			sql: "SELECT abc FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
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
			result: &xsql.GroupedTuplesSet{
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
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 GROUP BY id1, TUMBLINGWINDOW(ss, 10), f1",
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
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
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
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 GROUP BY meta(topic), TUMBLINGWINDOW(ss, 10)",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 1, "f1": "v1"},
						Metadata: xsql.Metadata{"topic": "topic1"},
					}, &xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 2, "f1": "v2"},
						Metadata: xsql.Metadata{"topic": "topic2"},
					}, &xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 3, "f1": "v1"},
						Metadata: xsql.Metadata{"topic": "topic1"},
					},
				},
			},

			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter:  "src1",
								Message:  xsql.Message{"id1": 1, "f1": "v1"},
								Metadata: xsql.Metadata{"topic": "topic1"},
							},
							&xsql.Tuple{
								Emitter:  "src1",
								Message:  xsql.Message{"id1": 3, "f1": "v1"},
								Metadata: xsql.Metadata{"topic": "topic1"},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter:  "src1",
								Message:  xsql.Message{"id1": 2, "f1": "v2"},
								Metadata: xsql.Metadata{"topic": "topic2"},
							},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
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
			result: &xsql.GroupedTuplesSet{
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
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
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
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY TUMBLINGWINDOW(ss, 10), src1.f1",
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
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
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
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY TUMBLINGWINDOW(ss, 10), src1.ts",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854573431)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)}},
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
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)}},
								},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.JoinTuple{
								Tuples: []xsql.TupleRow{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854573431)}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
								},
							},
						},
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), CASE WHEN id1 > 1 THEN \"others\" ELSE \"one\" END",
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
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							},
						},
					},
					{
						Content: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
				},
			},
		},

		{
			sql: "SELECT * FROM A FULL JOIN B on A.module=B.module FULL JOIN C on A.module=C.module GROUP BY A.module, TUMBLINGWINDOW(ss, 10)",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "B", Message: xsql.Message{"module": 1, "topic": "moduleB topic", "value": 1}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "C", Message: xsql.Message{"module": 1, "topic": "moduleC topic", "value": 100}},
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
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestFilterPlan_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		pp := &AggregateOp{Dimensions: stmt.Dimensions.GetGroups()}
		result := pp.Apply(ctx, tt.data, fv, afv)
		gr, ok := result.(*xsql.GroupedTuplesSet)
		if !ok {
			t.Errorf("result is not GroupedTuplesSet")
			continue
		}
		if tt.result.Len() != gr.Len() {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, gr)
			continue
		}

		for _, r := range tt.result.Groups {
			matched := false
			for _, gre := range gr.Groups {
				if reflect.DeepEqual(r, gre) {
					matched = true
				}
			}
			if !matched {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, gr)
			}
		}
	}
}

func TestAggregatePlanError(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result error
	}{
		{
			sql:    "SELECT abc FROM tbl group by abc",
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		},

		{
			sql: "SELECT abc FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 * 2",
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
			result: errors.New("run Group By error: invalid operation string(v1) * int64(2)"),
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
		pp := &AggregateOp{Dimensions: stmt.Dimensions.GetGroups()}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
