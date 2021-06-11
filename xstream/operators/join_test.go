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

func TestLeftJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   xsql.WindowTuplesSet
		result interface{}
	}{
		{ //0
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},
		{ // 1
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},
		{ // 2
			sql: "SELECT id1 FROM src1 left join src2 on src1.ts = src2.ts",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2", "ts": common.TimeFromUnixMilli(1568854525000)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v3", "ts": common.TimeFromUnixMilli(1568854535000)},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1", "ts": common.TimeFromUnixMilli(1568854515000)},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2", "ts": common.TimeFromUnixMilli(1568854525000)},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3", "ts": common.TimeFromUnixMilli(1568854545000)},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1", "ts": common.TimeFromUnixMilli(1568854515000)}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "ts": common.TimeFromUnixMilli(1568854525000)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2", "ts": common.TimeFromUnixMilli(1568854525000)}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3", "ts": common.TimeFromUnixMilli(1568854535000)}},
					},
				},
			},
		},
		{ // 3
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 5, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 6, "f2": "w3"},
						},
					},
				},
			},
			result: nil,
		},

		{ // 4
			sql: "SELECT id1 FROM src1 As s1 left join src2 as s2 on s1.id1 = s2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "s1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "s1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "s1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "s1",
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "s2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "s2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "s2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "s2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},

		{ // 5
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
					},
				},
			},
		},

		{ // 6
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples:  []xsql.Tuple{},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples:  nil,
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples:  []xsql.Tuple{},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: nil,
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples:  nil,
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1*2 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
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
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2*2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.f1->cid = src2.f2->cid",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": str2Map(`{"cid" : 3, "name" : "alice1"}`)},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": str2Map(`{"cid" : 4, "name" : "alice2"}`)},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": str2Map(`{"cid" : 3, "name" : "alice1"}`)}},
					},
				},
			},
		},

		{
			sql: "SELECT id1, mqtt(src1.topic) AS a, mqtt(src2.topic) as b FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 1, "f1": "v1"},
							Metadata: xsql.Metadata{"topic": "devices/type1/device001"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter:  "src2",
							Message:  xsql.Message{"id2": 1, "f2": "w1"},
							Metadata: xsql.Metadata{"topic": "devices/type2/device001"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}, Metadata: xsql.Metadata{"topic": "devices/type1/device001"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}, Metadata: xsql.Metadata{"topic": "devices/type2/device001"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v4"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 4, "f1": "v5"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 3, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 3, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v4"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v4"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 3, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 4, "f1": "v5"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestLeftJoinPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*xsql.Table); !ok {
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

func TestInnerJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   xsql.WindowTuplesSet
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 As s1 inner join src2 as s2 on s1.id1 = s2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "s1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "s1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "s1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "s1",
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "s2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "s2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "s2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "s2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples:  []xsql.Tuple{},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples:  nil,
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples:  []xsql.Tuple{},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples:  nil,
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: nil,
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1*2 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
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
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2*2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.f1->cid = src2.f2->cid",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": str2Map(`{"cid" : 3, "name" : "alice1"}`)},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": str2Map(`{"cid" : 4, "name" : "alice2"}`)},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": str2Map(`{"cid" : 1, "name" : "tom1"}`)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": str2Map(`{"cid" : 1, "name" : "tom2"}`)}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": str2Map(`{"cid" : 2, "name" : "mike1"}`)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": str2Map(`{"cid" : 2, "name" : "mike2"}`)}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 As s1 inner join src2 as s2 on s1.id1 = s2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "s1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "s1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "s1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "s1",
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "s2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "s2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "s2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "s2",
							Message: xsql.Message{"id2": 2, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "s1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "s2", Message: xsql.Message{"id2": 2, "f2": "w3"}},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestInnerJoinPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*xsql.Table); !ok {
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

func TestRightJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   xsql.WindowTuplesSet
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 1, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v3"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"f2": "w2"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 right join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestRightJoinPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*xsql.Table); !ok {
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

func TestFullJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   xsql.WindowTuplesSet
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w4"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w4"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples:  []xsql.Tuple{},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 full join src2 on src1.id1 = src2.id2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples:  []xsql.Tuple{},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestFullJoinPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*xsql.Table); !ok {
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

func TestCrossJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   xsql.WindowTuplesSet
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 cross join src2",
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},

				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
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
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v3"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 cross join src2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w2"},
						},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w1"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 1, "f2": "w2"}},
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestCrossJoinPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*xsql.Table); !ok {
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

func TestCrossJoinPlanError(t *testing.T) {
	var tests = []struct {
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
							Message: xsql.Message{"id1": 3, "f1": "v3"},
						},
					},
				},

				xsql.WindowTuples{
					Emitter: "src2",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src2",
							Message: xsql.Message{"id2": 1, "f2": "w1"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": "3", "f2": "w2"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 4, "f2": "w3"},
						}, {
							Emitter: "src2",
							Message: xsql.Message{"id2": 2, "f2": "w4"},
						},
					},
				},
			},
			result: errors.New("run Join error: invalid operation int64(1) = string(3)"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestCrossJoinPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		if table, ok := stmt.Sources[0].(*xsql.Table); !ok {
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

func str2Map(s string) map[string]interface{} {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(s), &input); err != nil {
		fmt.Printf("Failed to parse the JSON data.\n")
		return nil
	}
	return input
}
