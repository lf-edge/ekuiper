// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestOrderPlan_Apply(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "SELECT * FROM tbl WHERE abc*2+3 > 12 AND abc < 20 ORDER BY abc",
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
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
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
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"f1": "v2"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
		},
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": "2string", "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
			result: errors.New("run Order By error: incompatible types for comparison: int and string"),
		},
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY f1, id1 DESC",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
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
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
				},
			},
		},
		{
			sql: "SELECT * FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY ts DESC",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854525000)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854535000)},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854535000)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854525000)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY src1.id1 desc",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY src2.id2",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl group by abc ORDER BY def",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "tbl",
								Message: xsql.Message{
									"abc": int64(6),
									"def": "hello",
								},
							},
						},
					},
				},
			},
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "tbl",
								Message: xsql.Message{
									"abc": int64(6),
									"def": "hello",
								},
							},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 ORDER BY id1 desc",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
							},
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1"},
							},
						},
					},
					{
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
					},
				},
			},
			result: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2"},
							},
						},
					},
					{
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1"},
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
			sql: "SELECT count(*) as c FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 ORDER BY c",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.Row{
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
						Content: []xsql.Row{
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
						Content: []xsql.Row{
							&xsql.Tuple{
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
							},
						},
					},
					{
						Content: []xsql.Row{
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
		},
		{
			sql: "SELECT src2.id2 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10) ORDER BY src2.id2 DESC",
			data: &xsql.GroupedTuplesSet{
				Groups: []*xsql.GroupedTuples{
					{
						Content: []xsql.Row{
							&xsql.JoinTuple{
								Tuples: []xsql.Row{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.Row{
							&xsql.JoinTuple{
								Tuples: []xsql.Row{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.Row{
							&xsql.JoinTuple{
								Tuples: []xsql.Row{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
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
						Content: []xsql.Row{
							&xsql.JoinTuple{
								Tuples: []xsql.Row{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.Row{
							&xsql.JoinTuple{
								Tuples: []xsql.Row{
									&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
									&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
								},
							},
						},
						WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
					},
					{
						Content: []xsql.Row{
							&xsql.JoinTuple{
								Tuples: []xsql.Row{
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
			sql: "SELECT a FROM demo GROUP BY a, TUMBLINGWINDOW(ss, 10) ORDER BY a ASC",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 4},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 5},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 3},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 7},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 1},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 9},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 10},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 2},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 6},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 8},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 15},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 11},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 13},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 14},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 12},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 1},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 2},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 3},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 4},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 5},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 6},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 7},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 8},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 9},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 10},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 11},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 12},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 13},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 14},
					}, &xsql.Tuple{
						Emitter: "demo",
						Message: xsql.Message{"a": 15},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestOrderPlan_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
			assert.NoError(t, err)

			pp := &OrderOp{SortFields: stmt.SortFields}
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := pp.Apply(ctx, tt.data, fv, afv)
			assert.Equal(t, tt.result, result)
		})
	}
}
