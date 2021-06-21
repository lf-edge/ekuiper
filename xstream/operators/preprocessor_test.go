package operators

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"io/ioutil"
	"log"
	"path"
	"reflect"
	"testing"
	"time"
)

func TestPreprocessor_Apply(t *testing.T) {

	var tests = []struct {
		stmt   *xsql.StreamStmt
		data   []byte
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
			data:   []byte(`{"a": 6}`),
			result: errors.New("error in preprocessor: invalid data map[a:%!s(float64=6)], field abc not found"),
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
			},
			data:   []byte(`{"abc": null}`),
			result: errors.New("error in preprocessor: invalid data type for abc, expect bigint but found <nil>(<nil>)"),
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": 6}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": float64(6),
			},
			},
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
				"abc": 6,
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"abc": 6}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": float64(6),
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
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"abc": 34, "def" : "hello", "ghi": 50}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": float64(34),
				"def": "hello",
				"ghi": float64(50),
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
			data: []byte(`{"abc": "34", "def" : "hello", "ghi": "50"}`),
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
			data:   []byte(`{"abc": 77, "def" : "hello"}`),
			result: errors.New("error in preprocessor: invalid data type for def, expect boolean but found string(hello)"),
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
			},
			data:   []byte(`{"a": {"b" : "hello"}}`),
			result: errors.New("error in preprocessor: invalid data map[a:map[b:hello]], field abc not found"),
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": "hello",
				},
			},
			},
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
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
						},
					}},
				},
			},
			data: []byte(`{"a": "{\"b\" : \"32\"}"}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": float64(32),
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": {"b" : "32"}}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": "32",
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
			data: []byte(`{"a": []}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": make([]map[string]interface{}, 0),
			},
			},
		},
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
			data: []byte(`{"a": null}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []map[string]interface{}(nil),
			},
			},
		},
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
			data: []byte(`{"a": [null, {"b" : "hello2"}]}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []map[string]interface{}{
					nil,
					{"b": "hello2"},
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.ARRAY,
						FieldType: &xsql.ArrayType{
							Type: xsql.BIGINT,
						},
					}},
				},
			},
			data: []byte(`{"a": [[50, 60, 70],[66], [77]]}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": [][]int{
					{50, 60, 70},
					{66},
					{77},
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.ARRAY,
						FieldType: &xsql.ArrayType{
							Type: xsql.BIGINT,
						},
					}},
				},
			},
			data: []byte(`{"a": [null, [66], [77]]}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": [][]int{
					[]int(nil),
					{66},
					{77},
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": [{"b" : "hello1"}, {"b" : "hello2"}]}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []interface{}{
					map[string]interface{}{"b": "hello1"},
					map[string]interface{}{"b": "hello2"},
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.FLOAT,
					}},
				},
			},
			data: []byte(`{"a": "[\"55\", \"77\"]"}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []float64{
					55,
					77,
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": [55, 77]}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []interface{}{
					float64(55),
					float64(77),
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
			data: []byte(`{"a": null}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}(nil),
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
							{Name: "c", FieldType: &xsql.ArrayType{
								Type: xsql.FLOAT,
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": [35.2, 38.2]}}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": "hello",
					"c": []float64{
						35.2, 38.2,
					},
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
							{Name: "c", FieldType: &xsql.ArrayType{
								Type: xsql.FLOAT,
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": null}}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": "hello",
					"c": []float64(nil),
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
							{Name: "c", FieldType: &xsql.ArrayType{
								Type: xsql.FLOAT,
							}},
						},
					}},
				},
			},
			data:   []byte(`{"a": {"b" : "hello", "c": [null, 35.4]}}`),
			result: errors.New("error in preprocessor: fail to parse field c: invalid data type for [0], expect float but found <nil>(<nil>)"),
		},
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": {"b" : "hello", "c": {"d": 35.2}}}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": "hello",
					"c": map[string]interface{}{
						"d": 35.2,
					},
				},
			},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	contextLogger := common.Log.WithField("rule", "TestPreprocessor_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &Preprocessor{}
		pp.streamFields = convertFields(tt.stmt.StreamFields)

		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
			result := pp.Apply(ctx, tuple, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorTime_Apply(t *testing.T) {
	var tests = []struct {
		stmt   *xsql.StreamStmt
		data   []byte
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
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"abc": "2019-09-19T00:55:15.000Z", "def" : 1568854573431}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": "2019-09-19T00:55:15.000Z",
				"def": float64(1568854573431),
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
			data:   []byte(`{"abc": "2019-09-19T00:55:1dd5Z", "def" : 111568854573431}`),
			result: errors.New("error in preprocessor: invalid data type for abc, cannot convert to datetime: parsing time \"2019-09-19T00:55:1dd5Z\" as \"2006-01-02T15:04:05.000Z07:00\": cannot parse \"1dd5Z\" as \"05\""),
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.DATETIME}},
				},
				Options: &xsql.Options{
					DATASOURCE:       "users",
					FORMAT:           "JSON",
					KEY:              "USERID",
					CONF_KEY:         "srv1",
					TYPE:             "MQTT",
					TIMESTAMP:        "USERID",
					TIMESTAMP_FORMAT: "yyyy-MM-dd 'at' HH:mm:ss'Z'X",
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
	contextLogger := common.Log.WithField("rule", "TestPreprocessorTime_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &Preprocessor{}
		pp.streamFields = convertFields(tt.stmt.StreamFields)
		if tt.stmt.Options != nil {
			pp.timestampFormat = tt.stmt.Options.TIMESTAMP_FORMAT
		}
		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
			result := pp.Apply(ctx, tuple, fv, afv)
			//workaround make sure all the timezone are the same for time vars or the DeepEqual will be false.
			if rt, ok := result.(*xsql.Tuple); ok {
				if rtt, ok := rt.Message["abc"].(time.Time); ok {
					rt.Message["abc"] = rtt.UTC()
				}
			}
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func convertFields(o xsql.StreamFields) []interface{} {
	if o == nil {
		return nil
	}
	fields := make([]interface{}, len(o))
	for i, _ := range o {
		fields[i] = &o[i]
	}
	return fields
}

func TestPreprocessorEventtime_Apply(t *testing.T) {

	var tests = []struct {
		stmt   *xsql.StreamStmt
		data   []byte
		result interface{}
	}{
		//Basic type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
				Options: &xsql.Options{
					DATASOURCE:       "users",
					FORMAT:           "JSON",
					KEY:              "USERID",
					CONF_KEY:         "srv1",
					TYPE:             "MQTT",
					TIMESTAMP:        "abc",
					TIMESTAMP_FORMAT: "yyyy-MM-dd''T''HH:mm:ssX'",
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
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
				Options: &xsql.Options{
					DATASOURCE:       "users",
					FORMAT:           "JSON",
					KEY:              "USERID",
					CONF_KEY:         "srv1",
					TYPE:             "MQTT",
					TIMESTAMP:        "abc",
					TIMESTAMP_FORMAT: "yyyy-MM-dd''T''HH:mm:ssX'",
				},
			},
			data: []byte(`{"abc": 1568854515000}`),
			result: &xsql.Tuple{Message: xsql.Message{
				"abc": float64(1568854515000),
			}, Timestamp: 1568854515000,
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
				Options: &xsql.Options{
					DATASOURCE: "users",
					TIMESTAMP:  "abc",
				},
			},
			data:   []byte(`{"abc": true}`),
			result: errors.New("cannot convert timestamp field abc to timestamp with error unsupported type to convert to timestamp true"),
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
				},
				Options: &xsql.Options{
					DATASOURCE: "users",
					TIMESTAMP:  "def",
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
				Options: &xsql.Options{
					DATASOURCE: "users",
					TIMESTAMP:  "abc",
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
				Options: &xsql.Options{
					DATASOURCE:       "users",
					TIMESTAMP:        "def",
					TIMESTAMP_FORMAT: "yyyy-MM-dd'AT'HH:mm:ss",
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
				Options: &xsql.Options{
					DATASOURCE:       "users",
					TIMESTAMP:        "def",
					TIMESTAMP_FORMAT: "yyyy-MM-ddaHH:mm:ss",
				},
			},
			data:   []byte(`{"abc": 34, "def" : "2019-09-23AT02:47:29", "ghi": 50}`),
			result: errors.New("cannot convert timestamp field def to timestamp with error parsing time \"2019-09-23AT02:47:29\" as \"2006-01-02PM15:04:05\": cannot parse \"02:47:29\" as \"PM\""),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	contextLogger := common.Log.WithField("rule", "TestPreprocessorEventtime_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {

		pp := &Preprocessor{
			defaultFieldProcessor: defaultFieldProcessor{
				streamFields:    convertFields(tt.stmt.StreamFields),
				isBinary:        false,
				timestampFormat: tt.stmt.Options.TIMESTAMP_FORMAT,
			},
			isEventTime:    true,
			timestampField: tt.stmt.Options.TIMESTAMP,
		}

		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
			result := pp.Apply(ctx, tuple, fv, afv)
			//workaround make sure all the timezone are the same for time vars or the DeepEqual will be false.
			if rt, ok := result.(*xsql.Tuple); ok {
				if rtt, ok := rt.Message["abc"].(time.Time); ok {
					rt.Message["abc"] = rtt.UTC()
				}
			}
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorError(t *testing.T) {
	tests := []struct {
		stmt   *xsql.StreamStmt
		data   []byte
		result interface{}
	}{
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
			},
			data:   []byte(`{"abc": "dafsad"}`),
			result: errors.New("error in preprocessor: invalid data type for abc, expect bigint but found string(dafsad)"),
		}, {
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
			data:   []byte(`{"a": {"d" : "hello"}}`),
			result: errors.New("error in preprocessor: invalid data map[d:hello], field b not found"),
		}, {
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
				Options: &xsql.Options{
					DATASOURCE:       "users",
					FORMAT:           "JSON",
					KEY:              "USERID",
					CONF_KEY:         "srv1",
					TYPE:             "MQTT",
					TIMESTAMP:        "abc",
					TIMESTAMP_FORMAT: "yyyy-MM-dd''T''HH:mm:ssX'",
				},
			},
			data:   []byte(`{"abc": "not a time"}`),
			result: errors.New("error in preprocessor: invalid data type for abc, expect bigint but found string(not a time)"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	contextLogger := common.Log.WithField("rule", "TestPreprocessorError")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {

		pp := &Preprocessor{}
		pp.streamFields = convertFields(tt.stmt.StreamFields)
		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
			result := pp.Apply(ctx, tuple, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorForBinary(t *testing.T) {
	docsFolder, err := common.GetLoc("docs/")
	if err != nil {
		t.Errorf("Cannot find docs folder: %v", err)
	}
	image, err := ioutil.ReadFile(path.Join(docsFolder, "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	//TODO test bytea type conversion to string or else
	var tests = []struct {
		stmt     *xsql.StreamStmt
		data     []byte
		isBinary bool
		result   interface{}
	}{
		{
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data:     image,
			isBinary: true,
			result: &xsql.Tuple{Message: xsql.Message{
				"self": image,
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "img", FieldType: &xsql.BasicType{Type: xsql.BYTEA}},
				},
			},
			data:     image,
			isBinary: true,
			result: &xsql.Tuple{Message: xsql.Message{
				"img": image,
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.BYTEA}},
						},
					}},
				},
			},
			data: []byte(fmt.Sprintf(`{"a": {"b" : "%s"}}`, b64img)),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": map[string]interface{}{
					"b": image,
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.BYTEA,
					}},
				},
			},
			data: []byte(fmt.Sprintf(`{"a": ["%s"]}`, b64img)),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": [][]byte{
					image,
				},
			},
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.STRUCT,
						FieldType: &xsql.RecType{
							StreamFields: []xsql.StreamField{
								{Name: "b", FieldType: &xsql.BasicType{Type: xsql.BYTEA}},
							},
						},
					}},
				},
			},
			data: []byte(fmt.Sprintf(`{"a": [{"b":"%s"}]}`, b64img)),
			result: &xsql.Tuple{Message: xsql.Message{
				"a": []map[string]interface{}{
					{"b": image},
				},
			},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	contextLogger := common.Log.WithField("rule", "TestPreprocessorForBinary")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &Preprocessor{}
		pp.streamFields = convertFields(tt.stmt.StreamFields)
		pp.isBinary = tt.isBinary
		format := "json"
		if tt.isBinary {
			format = "binary"
		}
		if dm, e := common.MessageDecode(tt.data, format); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
			result := pp.Apply(ctx, tuple, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch", i, tuple)
			}
		}

	}
}
