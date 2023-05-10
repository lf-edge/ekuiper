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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func str2Map(s string) map[string]interface{} {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(s), &input); err != nil {
		fmt.Printf("Failed to parse the JSON data.\n")
		return nil
	}
	return input
}

func TestLeftJoinPlan_Apply(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.WindowTuples
		result interface{}
	}{
		{ // 0
			sql: "SELECT id1 FROM src1 left join src2 on id1 = id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
				WindowRange: xsql.NewWindowRange(1541152486013, 1541152487013),
			},
		},
		{ // 1
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},
		{ // 2
			sql: "SELECT id1 FROM src1 left join src2 on src1.ts = src2.ts",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854525000)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v3", "ts": cast.TimeFromUnixMilli(1568854535000)},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1", "ts": cast.TimeFromUnixMilli(1568854515000)},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2", "ts": cast.TimeFromUnixMilli(1568854525000)},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3", "ts": cast.TimeFromUnixMilli(1568854545000)},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1", "ts": cast.TimeFromUnixMilli(1568854515000)}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854525000)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2", "ts": cast.TimeFromUnixMilli(1568854525000)}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3", "ts": cast.TimeFromUnixMilli(1568854535000)}},
						},
					},
				},
			},
		},
		{ // 3
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 5, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 6, "f2": "w3"},
					},
				},
			},
			result: nil,
		},

		{ // 4
			sql: "SELECT id1 FROM src1 As s1 left join src2 as s2 on s1.id1 = s2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
					&xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{ // 5
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}}, &xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
						},
					},
				},
			},
		},

		{ // 6
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: nil,
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: nil,
		},

		{ // 10     select id1 FROM src1 left join src2 on null = null
			sql: "SELECT id1 FROM src1 left join src2 on src1.id2 = src2.id1",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f2": "w1"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f2": "w1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f2": "w1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1*2 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
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
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2*2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.f1->cid = src2.f2->cid",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": str2Map(`{"cid" : 3, "name" : "alice1"}`)},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": str2Map(`{"cid" : 4, "name" : "alice2"}`)},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": str2Map(`{"cid" : 3, "name" : "alice1"}`)}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1, mqtt(src1.topic) AS a, mqtt(src2.topic) as b FROM src1 left join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 1, "f1": "v1"},
						Metadata: xsql.Metadata{"topic": "devices/type1/device001"},
					},

					&xsql.Tuple{
						Emitter:  "src2",
						Message:  xsql.Message{"id2": 1, "f2": "w1"},
						Metadata: xsql.Metadata{"topic": "devices/type2/device001"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}, Metadata: xsql.Metadata{"topic": "devices/type1/device001"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}, Metadata: xsql.Metadata{"topic": "devices/type2/device001"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v4"},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 4, "f1": "v5"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 3, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 3, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v4"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v4"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 4, "f1": "v5"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestLeftJoinPlan_Apply")
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}

func TestInnerJoinPlan_Apply(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.WindowTuples
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 As s1 inner join src2 as s2 on s1.id1 = s2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
					&xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v2"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1*2 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
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
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2*2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.f1->cid = src2.f2->cid",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)},
					}, &xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": str2Map(`{"cid" : 3, "name" : "alice1"}`)},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": str2Map(`{"cid" : 4, "name" : "alice2"}`)},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 As s1 inner join src2 as s2 on s1.id1 = s2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					}, &xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					}, &xsql.Tuple{
						Emitter: "s1",
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
					&xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "s2",
						Message: xsql.Message{"id2": 2, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w3"}},
						},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestInnerJoinPlan_Apply")
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}

func TestRightJoinPlan_Apply(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.WindowTuples
		result interface{}
	}{
		{ // 0
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},
		{ // 1
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 1, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v3"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"f2": "w2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},
		{ // 2
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
				},
			},
		},
		{ // 3
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 1, "f1": "v3"},
					},
				},
			},
			result: nil,
		},
		{ // 4
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"f2": "w2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestRightJoinPlan_Apply")
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}

func TestFullJoinPlan_Apply(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.WindowTuples
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w4"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}}, &xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w4"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 5, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 6, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 5, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 6, "f2": "w3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestFullJoinPlan_Apply")
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}

func TestCrossJoinPlan_Apply(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.WindowTuples
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 cross join src2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},

					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
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
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 cross join src2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						},
					},
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}}, &xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 cross join src2",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: nil,
		},
		{
			sql: "SELECT id1 FROM src2 cross join src1",
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w2"},
					},
				},
			},
			result: nil,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestCrossJoinPlan_Apply")
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}

func TestCrossJoinPlanError(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql:    "SELECT id1 FROM src1 cross join src2",
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		}, {
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
						Message: xsql.Message{"id1": 3, "f1": "v3"},
					},

					&xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 1, "f2": "w1"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": "3", "f2": "w2"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 4, "f2": "w3"},
					}, &xsql.Tuple{
						Emitter: "src2",
						Message: xsql.Message{"id2": 2, "f2": "w4"},
					},
				},
			},
			result: errors.New("run Join error: invalid operation int64(1) = string(3)"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestCrossJoinPlan_Apply")
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			pp := &JoinOp{Joins: stmt.Joins, From: table}
			result := pp.Apply(ctx, tt.data, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
			}
		}
	}
}
