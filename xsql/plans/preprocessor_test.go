package plans

import (
	"encoding/json"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestPreprocessor_Apply(t *testing.T) {

	var tests = []struct {
		stmt *xsql.StreamStmt
		data []byte
		result interface{}
	}{
		//Basic type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
			},
			data: []byte(`{"a": 6}`),
			result: nil,
		},

		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
			},
			data: []byte(`{"abc": 6}`),
			result: &xsql.Tuple{Message: xsql.Message{
					"abc": int(6),
				},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
				},
			},
			data: []byte(`{"abc": 34, "def" : "hello", "ghi": 50}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": float64(34),
				"def": "hello",
				},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
			},
			data: []byte(`{"abc": 77, "def" : "hello"}`),
			result: nil,
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: nil,
		},
		//Rec type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: &xsql.Tuple{Message: xsql.Message{
					"a": map[string]interface{}{
					"b": "hello",
				},
			},
			},
		},
		//Array of complex type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.STRUCT,
						FieldType: &xsql.RecType{
							StreamFields: []xsql.StreamField{
								{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`{"a": [{"b" : "hello1"}, {"b" : "hello2"}]}`),
			result: &xsql.Tuple{Message: xsql.Message{
					"a": []map[string]interface{}{
						{"b": "hello1"},
						{"b": "hello2"},
					},
				},
			},
		},
		//Rec of complex type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
							{Name: "c", FieldType: &xsql.RecType{
								StreamFields: []xsql.StreamField{
									{Name: "d", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
								},
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": {"d": 35.2}}}`),
			result: &xsql.Tuple{Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": map[string]interface{}{
							"d": int(35),
						},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	for i, tt := range tests {

		pp := &Preprocessor{streamStmt: tt.stmt}

		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message:dm}
			result := pp.Apply(nil, tuple)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorTime_Apply(t *testing.T){
	var tests = []struct {
		stmt *xsql.StreamStmt
		data []byte
		result interface{}
	}{
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
				},
			},
			data: []byte(`{"abc": "2019-09-19T00:55:15.000Z", "def" : 1568854573431}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": common.TimeFromUnixMilli(1568854515000),
				"def": common.TimeFromUnixMilli(1568854573431),
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
				},
			},
			data: []byte(`{"abc": "2019-09-19T00:55:1dd5Z", "def" : 111568854573431}`),
			result: nil,
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"FORMAT" : "AVRO",
					"KEY" : "USERID",
					"CONF_KEY" : "srv1",
					"TYPE" : "MQTT",
					"TIMESTAMP" : "USERID",
					"TIMESTAMP_FORMAT" : "yyyy-MM-dd 'at' HH:mm:ss'Z'X",
				},
			},
			data: []byte(`{"abc": "2019-09-19 at 18:55:15Z+07", "def" : 1568854573431}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": common.TimeFromUnixMilli(1568894115000),
				"def": common.TimeFromUnixMilli(1568854573431),
			}},
		},
		//Array type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.DATETIME,
					}},
				},
			},
			data: []byte(`{"a": [1568854515123, 1568854573431]}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []time.Time{
					common.TimeFromUnixMilli(1568854515123),
					common.TimeFromUnixMilli(1568854573431),
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
							{Name: "c", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": 1568854515000}}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": "hello",
					"c": common.TimeFromUnixMilli(1568854515000),
				},
			},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	for i, tt := range tests {

		pp := &Preprocessor{streamStmt: tt.stmt}

		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message:dm}
			result := pp.Apply(nil, tuple)
			//workaround make sure all the timezone are the same for time vars or the DeepEqual will be false.
			if rt, ok := result.(*xsql.Tuple); ok{
				if rtt, ok := rt.Message["abc"].(time.Time); ok{
					rt.Message["abc"] = rtt.UTC()
				}
			}
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorEventtime_Apply(t *testing.T) {

	var tests = []struct {
		stmt *xsql.StreamStmt
		data []byte
		result interface{}
	}{
		//Basic type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"FORMAT" : "AVRO",
					"KEY" : "USERID",
					"CONF_KEY" : "srv1",
					"TYPE" : "MQTT",
					"TIMESTAMP" : "abc",
					"TIMESTAMP_FORMAT" : "yyyy-MM-dd''T''HH:mm:ssX'",
				},
			},
			data: []byte(`{"abc": 1568854515000}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": int(1568854515000),
			}, Timestamp: 1568854515000,
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"TIMESTAMP" : "abc",
				},
			},
			data: []byte(`{"abc": true}`),
			result: nil,
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"TIMESTAMP" : "def",
				},
			},
			data: []byte(`{"abc": 34, "def" : "2019-09-23T02:47:29.754Z", "ghi": 50}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": float64(34),
				"def": "2019-09-23T02:47:29.754Z",
			}, Timestamp: int64(1569206849754),
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"TIMESTAMP" : "abc",
				},
			},
			data: []byte(`{"abc": "2019-09-19T00:55:15.000Z", "def" : 1568854573431}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": common.TimeFromUnixMilli(1568854515000),
				"def": common.TimeFromUnixMilli(1568854573431),
			}, Timestamp: int64(1568854515000),
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"TIMESTAMP" : "def",
					"TIMESTAMP_FORMAT" : "yyyy-MM-dd'AT'HH:mm:ss",
				},
			},
			data: []byte(`{"abc": 34, "def" : "2019-09-23AT02:47:29", "ghi": 50}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": float64(34),
				"def": "2019-09-23AT02:47:29",
			}, Timestamp: int64(1569206849000),
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"TIMESTAMP" : "def",
					"TIMESTAMP_FORMAT" : "yyyy-MM-ddaHH:mm:ss",
				},
			},
			data: []byte(`{"abc": 34, "def" : "2019-09-23AT02:47:29", "ghi": 50}`),
			result: nil,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	for i, tt := range tests {

		pp, err := NewPreprocessor(tt.stmt, true)
		if err != nil{
			t.Error(err)
		}

		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message:dm}
			result := pp.Apply(nil, tuple)
			//workaround make sure all the timezone are the same for time vars or the DeepEqual will be false.
			if rt, ok := result.(*xsql.Tuple); ok{
				if rtt, ok := rt.Message["abc"].(time.Time); ok{
					rt.Message["abc"] = rtt.UTC()
				}
			}
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}