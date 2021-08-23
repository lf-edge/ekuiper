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

package operator

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"strings"
	"testing"
)

func TestMultiJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   xsql.WindowTuplesSet
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 left join src3 on src2.id2 = src3.id3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v3"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 1, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 inner join src3 on src2.id2 = src3.id3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v3"},
							},
						},
					},
					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 1, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
				WindowRange: &xsql.WindowRange{
					WindowStart: 1541152486013,
					WindowEnd:   1541152487013,
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
						},
					},
				},
				WindowRange: &xsql.WindowRange{
					WindowStart: 1541152486013,
					WindowEnd:   1541152487013,
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 inner join src3 on src1.id1 = src3.id3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 5, "f1": "v5"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 2, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 5, "f1": "v5"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 5, "f3": "x5"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 full join src3 on src1.id1 = src3.id3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 5, "f1": "v5"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 2, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 5, "f1": "v5"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 5, "f3": "x5"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 2, "f3": "x1"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 right join src3 on src2.id2 = src3.id3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v3"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 1, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 5, "f3": "x5"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 right join src3 on src2.id2 = src3.id3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v3"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 1, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w3"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v3"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 1, "f3": "x1"}},
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v3"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w3"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src3", Message: xsql.Message{"id3": 5, "f3": "x5"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 cross join src3",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 5, "f1": "v5"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id2": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id2": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id3": 2, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id3": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 2, "f3": "x1"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}}, {Emitter: "src3", Message: xsql.Message{"id3": 5, "f3": "x5"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 5, "f1": "v5"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 2, "f3": "x1"}},
						},
					},

					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 5, "f1": "v5"}},
							{Emitter: "src3", Message: xsql.Message{"id3": 5, "f3": "x5"}},
						},
					},
				},
			},
		}, {
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id = src2.id inner join src3 on src1.id = src3.id",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id": 1, "f1": "v1"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id": 2, "f1": "v5"},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id": 3, "f1": "v3"},
							},
						},
					},

					{
						Emitter: "src2",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src2",
								Message: xsql.Message{"id": 1, "f2": "w1"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id": 2, "f2": "w2"},
							}, {
								Emitter: "src2",
								Message: xsql.Message{"id": 4, "f2": "w3"},
							},
						},
					},

					{
						Emitter: "src3",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src3",
								Message: xsql.Message{"id": 1, "f3": "x1"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id": 1, "f3": "x3"},
							}, {
								Emitter: "src3",
								Message: xsql.Message{"id": 5, "f3": "x5"},
							},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "f2": "w1"}},
							{Emitter: "src3", Message: xsql.Message{"id": 1, "f3": "x1"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id": 1, "f2": "w1"}},
							{Emitter: "src3", Message: xsql.Message{"id": 1, "f3": "x3"}},
						},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestMultiJoinPlan_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*ast.Table); !ok {
			t.Errorf("statement source is not a table")
		} else {
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}
