package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"strings"
	"testing"
)

func TestHashFunc_Apply1(t *testing.T) {
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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestHashFunc_Apply1")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectPlan{Fields: stmt.Fields}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

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
		data   xsql.JoinTupleSets
		result []map[string]interface{}
	}{
		{
			sql: "SELECT id1, mqtt(src1.topic) AS a, mqtt(src2.topic) as b FROM src1 LEFT JOIN src2 ON src1.id1 = src2.id1",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": "1", "f1": "v1"}, Metadata: xsql.Metadata{"topic": "devices/type1/device001"}},
						{Emitter: "src2", Message: xsql.Message{"id2": "1", "f2": "w1"}, Metadata: xsql.Metadata{"topic": "devices/type2/device001"}},
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
	contextLogger := common.Log.WithField("rule", "TestMqttFunc_Apply2")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectPlan{Fields: stmt.Fields}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestHashFunc_Apply1")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectPlan{Fields: stmt.Fields}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}
