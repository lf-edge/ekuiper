package plans

import (
	"encoding/json"
	"engine/xsql"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestLeftJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql  string
		data xsql.MultiEmitterTuples
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 2, "f2" : "w2",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 3, "f1" : "v3",},},
					},
				},
			},
		},


		{
			sql: "SELECT id1 FROM src1 As s1 left join src2 as s2 on s1.id1 = s2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"s1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"s2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "s1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "s2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "s1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "s2", Message: map[string]interface{}{ "id2" : 2, "f2" : "w2",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "s1", Message: map[string]interface{}{ "id1" : 3, "f1" : "v3",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 1, "f2" : "w2", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w2",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{

					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 3, "f1" : "v3",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter: "src2",
					Messages: nil,
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 3, "f1" : "v3",},},
					},
				},
			},
		},


		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{} {},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 1, "f2" : "w2", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:nil,
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 1, "f2" : "w2", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1*2 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 2, "f2" : "w2",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 4, "f2" : "w3",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 3, "f1" : "v3",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2*2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1", },},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 3, "f1" : "v3",},},
					},
				},
			},
		},


		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.f1->cid = src2.f2->cid",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : str2Map(`{"cid" : 1, "name" : "tom1"}`), },
						{ "id1" : 2, "f1" : str2Map(`{"cid" : 2, "name" : "mike1"}`), },
						{ "id1" : 3, "f1" : str2Map(`{"cid" : 3, "name" : "alice1"}`), },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : str2Map(`{"cid" : 1, "name" : "tom2"}`),},
						{ "id2" : 2, "f2" : str2Map(`{"cid" : 2, "name" : "mike2"}`), },
						{ "id2" : 4, "f2" : str2Map(`{"cid" : 4, "name" : "alice2"}`), },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : str2Map(`{"cid" : 1, "name" : "tom1"}`), },},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : str2Map(`{"cid" : 1, "name" : "tom2"}`), },},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : str2Map(`{"cid" : 2, "name" : "mike1"}`), },},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 2, "f2" : str2Map(`{"cid" : 2, "name" : "mike2"}`), },},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 3, "f1" : str2Map(`{"cid" : 3, "name" : "alice1"}`), },},
					},
				},
			},
		},

	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		pp := &JoinPlan{Joins : stmt.Joins}
		result := pp.Apply(nil, tt.data)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestInnerJoinPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql  string
		data xsql.MultiEmitterTuples
		result interface{}
	}{
		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 2, "f2" : "w2",},},
					},
				},
			},
		},


		{
			sql: "SELECT id1 FROM src1 As s1 inner join src2 as s2 on s1.id1 = s2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"s1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"s2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "s1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "s2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "s1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "s2", Message: map[string]interface{}{ "id2" : 2, "f2" : "w2",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 1, "f2" : "w2", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w2",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{

					},
				},
			},
			result: xsql.MergedEmitterTupleSets{},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter: "src2",
					Messages: nil,
				},
			},
			result: xsql.MergedEmitterTupleSets{},
		},


		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{} {},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 1, "f2" : "w2", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{ },
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:nil,
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 1, "f2" : "w2", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1*2 = src2.id2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : "v1",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 2, "f2" : "w2",},},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 4, "f2" : "w3",},},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.id1 = src2.id2*2",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : "v1", },
						{ "id1" : 2, "f1" : "v2", },
						{ "id1" : 3, "f1" : "v3", },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : "w1", },
						{ "id2" : 2, "f2" : "w2", },
						{ "id2" : 4, "f2" : "w3", },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : "v2",},},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : "w1", },},
					},
				},
			},
		},


		{
			sql: "SELECT id1 FROM src1 inner join src2 on src1.f1->cid = src2.f2->cid",
			data: xsql.MultiEmitterTuples{
				xsql.EmitterTuples{
					Emitter:"src1",
					Messages:[]map[string]interface{}{
						{ "id1" : 1, "f1" : str2Map(`{"cid" : 1, "name" : "tom1"}`), },
						{ "id1" : 2, "f1" : str2Map(`{"cid" : 2, "name" : "mike1"}`), },
						{ "id1" : 3, "f1" : str2Map(`{"cid" : 3, "name" : "alice1"}`), },
					},
				},

				xsql.EmitterTuples{
					Emitter:"src2",
					Messages:[]map[string]interface{}{
						{ "id2" : 1, "f2" : str2Map(`{"cid" : 1, "name" : "tom2"}`),},
						{ "id2" : 2, "f2" : str2Map(`{"cid" : 2, "name" : "mike2"}`), },
						{ "id2" : 4, "f2" : str2Map(`{"cid" : 4, "name" : "alice2"}`), },
					},
				},
			},
			result: xsql.MergedEmitterTupleSets{
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 1, "f1" : str2Map(`{"cid" : 1, "name" : "tom1"}`), },},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 1, "f2" : str2Map(`{"cid" : 1, "name" : "tom2"}`), },},
					},
				},
				xsql.MergedEmitterTuple{
					MergedMessage: []xsql.EmitterTuple{
						{Emitter: "src1", Message: map[string]interface{}{ "id1" : 2, "f1" : str2Map(`{"cid" : 2, "name" : "mike1"}`), },},
						{Emitter: "src2", Message: map[string]interface{}{ "id2" : 2, "f2" : str2Map(`{"cid" : 2, "name" : "mike2"}`), },},
					},
				},
			},
		},

	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		pp := &JoinPlan{Joins : stmt.Joins}
		result := pp.Apply(nil, tt.data)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
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
