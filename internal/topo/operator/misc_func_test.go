// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestMiscFunc_Apply1(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.Tuple
		result []map[string]interface{}
	}{
		{
			sql: "SELECT md5(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "The quick brown fox jumps over the lazy dog",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("9E107D9D372BB6826BD81D3542A419D6"),
			}},
		},
		{
			sql: "SELECT md5(d) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "The quick brown fox jumps over the lazy dog",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{}},
		},
		{
			sql: "SELECT sha1(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "The quick brown fox jumps over the lazy dog",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("2FD4E1C67A2D28FCED849EE1BB76E7391B93EB12"),
			}},
		},
		{
			sql: "SELECT sha256(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "The quick brown fox jumps over the lazy dog",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("D7A8FBB307D7809469CA9ABCB0082E4F8D5651E46D3CDB762D02D0BF37C9E592"),
			}},
		},
		{
			sql: "SELECT sha384(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "The quick brown fox jumps over the lazy dog",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("CA737F1014A48F4C0B6DD43CB177B0AFD9E5169367544C494011E3317DBF9A509CB1E5DC1E85A941BBEE3D7F2AFBC9B1"),
			}},
		},
		{
			sql: "SELECT sha512(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "The quick brown fox jumps over the lazy dog",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("07E547D9586F6A73F73FBAC0435ED76951218FB7D0C8D788A309D785436BBB642E93A252A954F23912547D1E8A3B5ED6E1BFD7097821233FA0538F3DB854FEE6"),
			}},
		},

		{
			sql: "SELECT mqtt(topic) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
				Metadata: xsql.Metadata{
					"topic": "devices/device_001/message",
				},
			},
			result: []map[string]interface{}{{
				"a": "devices/device_001/message",
			}},
		},

		{
			sql: "SELECT mqtt(topic) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
				Metadata: xsql.Metadata{
					"topic": "devices/device_001/message",
				},
			},
			result: []map[string]interface{}{{
				"a": "devices/device_001/message",
			}},
		},

		{
			sql: "SELECT topic, mqtt(topic) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"topic": "fff",
				},
				Metadata: xsql.Metadata{
					"topic": "devices/device_001/message",
				},
			},
			result: []map[string]interface{}{{
				"topic": "fff",
				"a":     "devices/device_001/message",
			}},
		},

		{
			sql: "SELECT cardinality(arr) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
					"arr":         []int{},
				},
			},
			result: []map[string]interface{}{{
				"r": 0,
			}},
		},

		{
			sql: "SELECT cardinality(arr) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
					"arr":         []int{1, 2, 3, 4, 5},
				},
			},
			result: []map[string]interface{}{{
				"r": 5,
			}},
		},

		{
			sql: "SELECT isNull(arr) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
					"arr":         []int{},
				},
			},
			result: []map[string]interface{}{{
				"r": false,
			}},
		},
		{
			sql: "SELECT isNull(arr) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
					"arr":         []float64(nil),
				},
			},
			result: []map[string]interface{}{{
				"r": true,
			}},
		},

		{
			sql: "SELECT isNull(rec) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
					"rec":         map[string]interface{}(nil),
				},
			},
			result: []map[string]interface{}{{
				"r": true,
			}},
		},
		{
			sql: "SELECT cast(a * 1000, \"datetime\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": 1.62000273e+09,
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": cast.TimeFromUnixMilli(1.62000273e+12),
			}},
		},
		{
			sql: "SELECT rule_id() AS rule_id FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{},
			},
			result: []map[string]interface{}{{
				"rule_id": "rule0",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestMiscFunc_Apply1")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ctx = ctx.WithMeta("rule0", "op1", &state.MemoryStore{}).(*context.DefaultContext)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestMqttFunc_Apply2(t *testing.T) {
	tests := []struct {
		sql    string
		data   *xsql.JoinTuples
		result []map[string]interface{}
	}{
		{
			sql: "SELECT id1, mqtt(src1.topic) AS a, mqtt(src2.topic) as b FROM src1 LEFT JOIN src2 ON src1.id1 = src2.id1",
			data: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{Emitter: "src1", Message: xsql.Message{"id1": "1", "f1": "v1"}, Metadata: xsql.Metadata{"topic": "devices/type1/device001"}},
							&xsql.Tuple{Emitter: "src2", Message: xsql.Message{"id2": "1", "f2": "w1"}, Metadata: xsql.Metadata{"topic": "devices/type2/device001"}},
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"id1": "1",
				"a":   "devices/type1/device001",
				"b":   "devices/type2/device001",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestMqttFunc_Apply2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestMetaFunc_Apply1(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "SELECT topic, meta(topic) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"topic": "fff",
				},
				Metadata: xsql.Metadata{
					"topic": "devices/device_001/message",
				},
			},
			result: []map[string]interface{}{{
				"topic": "fff",
				"a":     "devices/device_001/message",
			}},
		},
		{
			sql: "SELECT meta(device) as d, meta(temperature->device) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
				},
				Metadata: xsql.Metadata{
					"temperature": map[string]interface{}{
						"id":     "dfadfasfas",
						"device": "device2",
					},
					"device": "gateway",
				},
			},
			result: []map[string]interface{}{{
				"d": "gateway",
				"r": "device2",
			}},
		},
		{
			sql: "SELECT meta(*) as r FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"temperature": 43.2,
				},
				Metadata: xsql.Metadata{
					"temperature": map[string]interface{}{
						"id":     "dfadfasfas",
						"device": "device2",
					},
					"device": "gateway",
				},
			},
			result: []map[string]interface{}{{
				"r": map[string]interface{}{
					"temperature": map[string]interface{}{
						"id":     "dfadfasfas",
						"device": "device2",
					},
					"device": "gateway",
				},
			}},
		},
		{
			sql: "SELECT topic, meta(`Light-diming`->device) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"topic": "fff",
				},
				Metadata: xsql.Metadata{
					"Light-diming": map[string]interface{}{
						"device": "device2",
					},
				},
			},
			result: []map[string]interface{}{{
				"topic": "fff",
				"a":     "device2",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestMetaFunc_Apply1")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestJsonPathFunc_Apply1(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result interface{}
		err    string
	}{
		{
			sql: `SELECT json_path_query(equipment, "$.arm_right") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []map[string]interface{}{
							{
								"name":   "ring of despair",
								"weight": 0.1,
							}, {
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": "Sword of flame",
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.rings[*].weight") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": []interface{}{
					0.1, 2.4,
				},
			}},
		}, {
			sql: `SELECT json_path_query_first(equipment, "$.rings[*].weight") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": 0.1,
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.rings[? @.weight>1]") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": []interface{}{
					map[string]interface{}{
						"name":   "ring of strength",
						"weight": 2.4,
					},
				},
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.rings[? @.weight>1].name") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": []interface{}{
					"ring of strength",
				},
			}},
		}, {
			sql: `SELECT json_path_exists(equipment, "$.rings[? @.weight>5]") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": false,
			}},
		}, {
			sql: `SELECT json_path_exists(equipment, "$.ring1") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": false,
			}},
		}, {
			sql: `SELECT json_path_exists(equipment, "$.rings") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []interface{}{
							map[string]interface{}{
								"name":   "ring of despair",
								"weight": 0.1,
							}, map[string]interface{}{
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": true,
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.rings[? (@.weight>1)].name") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []map[string]interface{}{
							{
								"name":   "ring of despair",
								"weight": 0.1,
							}, {
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": []interface{}{
					"ring of strength",
				},
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.rings[*]") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []float64{
							0.1, 2.4,
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": []interface{}{
					0.1, 2.4,
				},
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.rings") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": map[string]interface{}{
						"rings": []float64{
							0.1, 2.4,
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			result: []map[string]interface{}{{
				"a": []interface{}{
					0.1, 2.4,
				},
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$[0].rings[1]") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": []map[string]interface{}{
						{
							"rings": []float64{
								0.1, 2.4,
							},
							"arm_right": "Sword of flame",
							"arm_left":  "Shield of faith",
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"a": 2.4,
			}},
		}, {
			sql: "SELECT json_path_query(equipment, \"$[0][\\\"arm.left\\\"]\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment": []map[string]interface{}{
						{
							"rings": []float64{
								0.1, 2.4,
							},
							"arm.right": "Sword of flame",
							"arm.left":  "Shield of faith",
						},
					},
				},
			},
			result: []map[string]interface{}{{
				"a": "Shield of faith",
			}},
		}, {
			sql: "SELECT json_path_query(equipment, \"$[\\\"arm.left\\\"]\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class":     "warrior",
					"equipment": `{"rings": [0.1, 2.4],"arm.right": "Sword of flame","arm.left":  "Shield of faith"}`,
				},
			},
			result: []map[string]interface{}{{
				"a": "Shield of faith",
			}},
		}, {
			sql: "SELECT json_path_query(equipment, \"$[0][\\\"arm.left\\\"]\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class":     "warrior",
					"equipment": `[{"rings": [0.1, 2.4],"arm.right": "Sword of flame","arm.left":  "Shield of faith"}]`,
				},
			},
			result: []map[string]interface{}{{
				"a": "Shield of faith",
			}},
		}, {
			sql: `SELECT all[poi[-1] + 1]->ts as powerOnTs FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"all": []map[string]interface{}{
						{"SystemPowerMode": 0, "VehicleSpeed": 0, "FLWdwPosition": 0, "FrontWiperSwitchStatus": float64(1), "ts": 0},
						{"SystemPowerMode": 0, "VehicleSpeed": 0, "FLWdwPosition": 0, "FrontWiperSwitchStatus": float64(4), "ts": 500},
						{"SystemPowerMode": 2, "VehicleSpeed": 0, "FLWdwPosition": 0, "FrontWiperSwitchStatus": 0, "ts": 1000},
						{"SystemPowerMode": 2, "VehicleSpeed": 10, "FLWdwPosition": 20, "FrontWiperSwitchStatus": 0, "ts": 60000},
						{"SystemPowerMode": 2, "VehicleSpeed": 10, "FLWdwPosition": 20, "FrontWiperSwitchStatus": 0, "ts": 89500},
						{"SystemPowerMode": 2, "VehicleSpeed": 20, "FLWdwPosition": 50, "FrontWiperSwitchStatus": 5, "ts": 90000},
						{"SystemPowerMode": 2, "VehicleSpeed": 40, "FLWdwPosition": 60, "FrontWiperSwitchStatus": 5, "ts": 121000},
					},
					"poi": []interface{}{0, 1},
				},
			},
			result: []map[string]interface{}{{
				"powerOnTs": 1000,
			}},
		}, {
			sql: `SELECT json_path_query(equipment, "$.arm_right") AS a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"class": "warrior",
					"equipment2": map[string]interface{}{
						"rings": []map[string]interface{}{
							{
								"name":   "ring of despair",
								"weight": 0.1,
							}, {
								"name":   "ring of strength",
								"weight": 2.4,
							},
						},
						"arm_right": "Sword of flame",
						"arm_left":  "Shield of faith",
					},
				},
			},
			err: "run Select error: call func json_path_query error: invalid data nil for jsonpath",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestJsonFunc_Apply1")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		if rt, ok := opResult.(error); ok {
			if tt.err == "" {
				t.Errorf("%d: got error:\n  exp=%s\n  got=%s\n\n", i, tt.result, rt)
			} else if !reflect.DeepEqual(tt.err, testx.Errstring(rt)) {
				t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, rt)
			}
		} else {
			result, _ := parseResult(opResult, pp.IsAggregate)
			if tt.err == "" {
				if !reflect.DeepEqual(tt.result, result) {
					t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
				}
			} else {
				t.Errorf("%d: invalid result:\n  exp error %s\n  got=%s\n\n", i, tt.err, result)
			}
		}
	}
}

func TestChangedFuncs_Apply1(t *testing.T) {
	tests := []struct {
		sql    string
		data   []interface{}
		result [][]map[string]interface{}
	}{
		{
			sql: `SELECT changed_col(true, a), b FROM test`,
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: [][]map[string]interface{}{{{
				"changed_col": "a1",
				"b":           "b1",
			}}, {{
				"b": "b2",
			}}, {{}}, {{
				"b": "b2",
			}}},
		}, {
			sql: `SELECT changed_col(true, *) FROM test`,
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: [][]map[string]interface{}{{{
				"changed_col": map[string]interface{}{
					"a": "a1",
					"b": "b1",
				},
			}}, {{
				"changed_col": map[string]interface{}{
					"a": "a1",
					"c": "c1",
				},
			}}, {{}}, {{
				"changed_col": map[string]interface{}{
					"a": "a1",
					"b": "b2",
					"c": "c2",
				},
			}}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestChangedFuncs_Apply1")

	for i, tt := range tests {
		tempStore, _ := state.CreateStore("mockRule"+strconv.Itoa(i), api.AtMostOnce)
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("mockRule"+strconv.Itoa(i), "project", tempStore)
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
		r := make([][]map[string]interface{}, 0, len(tt.data))
		for _, d := range tt.data {
			opResult := pp.Apply(ctx, d, fv, afv)
			result, err := parseResult(opResult, pp.IsAggregate)
			if err != nil {
				t.Errorf("parse result error： %s", err)
				continue
			}
			r = append(r, result)
		}
		if !reflect.DeepEqual(tt.result, r) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, r)
		}
	}
}

func TestLagFuncs_Apply1(t *testing.T) {
	tests := []struct {
		sql    string
		data   []interface{}
		result [][]map[string]interface{}
	}{
		{
			sql: `SELECT lag(a) as a, lag(b) as b FROM test`,
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: [][]map[string]interface{}{{{}}, {{
				"a": "a1",
				"b": "b1",
			}}, {{
				"a": "a1",
				"b": "b2",
			}}, {{
				"a": "a1",
			}}},
		},

		{
			sql: `SELECT lag(a, 2, "a10") as a FROM test`,
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a2",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: [][]map[string]interface{}{{{
				"a": "a10",
			}}, {{
				"a": "a10",
			}}, {{
				"a": "a1",
			}}, {{
				"a": "a2",
			}}},
		},

		{
			sql: `SELECT lag(a, 2) as a FROM test`,
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a2",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: [][]map[string]interface{}{{{}}, {{}}, {{
				"a": "a1",
			}}, {{
				"a": "a2",
			}}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestChangedFuncs_Apply1")

	for i, tt := range tests {
		tempStore, _ := state.CreateStore("mockRule"+strconv.Itoa(i), api.AtMostOnce)
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("mockRule"+strconv.Itoa(i), "project", tempStore)
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
		r := make([][]map[string]interface{}, 0, len(tt.data))
		for _, d := range tt.data {
			opResult := pp.Apply(ctx, d, fv, afv)
			result, err := parseResult(opResult, pp.IsAggregate)
			if err != nil {
				t.Errorf("parse result error： %s", err)
				continue
			}
			r = append(r, result)
		}

		if !reflect.DeepEqual(tt.result, r) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, r)
		}
	}
}
