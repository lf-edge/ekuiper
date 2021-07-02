package operator

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"reflect"
	"strings"
	"testing"
)

func TestMiscFunc_Apply1(t *testing.T) {
	var tests = []struct {
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
				"r": float64(0),
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
				"r": float64(5),
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
				"a": "2021-05-03T00:45:30Z",
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestMiscFunc_Apply1")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{Fields: stmt.Fields}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}
func TestMqttFunc_Apply2(t *testing.T) {
	var tests = []struct {
		sql    string
		data   *xsql.JoinTupleSets
		result []map[string]interface{}
	}{
		{
			sql: "SELECT id1, mqtt(src1.topic) AS a, mqtt(src2.topic) as b FROM src1 LEFT JOIN src2 ON src1.id1 = src2.id1",
			data: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": "1", "f1": "v1"}, Metadata: xsql.Metadata{"topic": "devices/type1/device001"}},
							{Emitter: "src2", Message: xsql.Message{"id2": "1", "f2": "w1"}, Metadata: xsql.Metadata{"topic": "devices/type2/device001"}},
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
		pp := &ProjectOp{Fields: stmt.Fields}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}

func TestMetaFunc_Apply1(t *testing.T) {
	var tests = []struct {
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
		pp := &ProjectOp{Fields: stmt.Fields}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}

func TestJsonPathFunc_Apply1(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
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
		pp := &ProjectOp{Fields: stmt.Fields}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		result := pp.Apply(ctx, tt.data, fv, afv)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["kuiper_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte but found %v", result)
		}
	}
}
