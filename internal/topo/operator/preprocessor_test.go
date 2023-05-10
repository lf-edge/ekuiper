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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

func TestPreprocessor_Apply(t *testing.T) {
	tests := []struct {
		stmt   *ast.StreamStmt
		data   []byte
		result interface{}
	}{
		// Basic type
		{ // 0
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
			},
			data:   []byte(`{"a": 6}`),
			result: errors.New("error in preprocessor: field abc is not found"),
		},
		{ // 1
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
			},
			data:   []byte(`{"abc": null}`),
			result: errors.New("error in preprocessor: field abc type mismatch: cannot convert <nil>(<nil>) to int64"),
		},
		{ // 2
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": 6}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": float64(6),
				},
			},
		},
		{ // 3
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
			},
			data: []byte(`{"abc": 6}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
		},
		{ // 4
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"abc": 6}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": float64(6),
				},
			},
		},
		{ // 5
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
					{Name: "dEf", FieldType: &ast.BasicType{Type: ast.STRINGS}},
				},
			},
			data: []byte(`{"abc": 34, "def" : "hello", "ghi": 50}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": float64(34),
					"dEf": "hello",
					"def": "hello",
					"ghi": float64(50),
				},
			},
		},
		{ // 6
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"abc": 34, "def" : "hello", "ghi": 50}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": float64(34),
					"def": "hello",
					"ghi": float64(50),
				},
			},
		},
		{ // 7
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.STRINGS}},
				},
			},
			data:   []byte(`{"abc": "34", "def" : "hello", "ghi": "50"}`),
			result: errors.New("error in preprocessor: field abc type mismatch: cannot convert string(34) to float64"),
		},
		{ // 8
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.BOOLEAN}},
				},
			},
			data:   []byte(`{"abc": 77, "def" : "hello"}`),
			result: errors.New("error in preprocessor: field def type mismatch: cannot convert string(hello) to bool"),
		},
		{ // 9
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
				},
			},
			data:   []byte(`{"a": {"b" : "hello"}}`),
			result: errors.New("error in preprocessor: field abc is not found"),
		},
		{ // 10
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
					},
				},
			},
		},
		// Rec type
		{ // 11
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
					},
				},
			},
		},
		{ // 12
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.FLOAT}},
						},
					}},
				},
			},
			data:   []byte(`{"a": "{\"b\" : \"32\"}"}`),
			result: errors.New("error in preprocessor: field a type mismatch: field b type mismatch: cannot convert string(32) to float64"),
		},
		{ // 13
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": {"b" : "32"}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "32",
					},
				},
			},
		},
		// Array of complex type
		{ // 14
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`{"a": [{"b" : "hello1"}, {"b" : "hello2"}]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						map[string]interface{}{"b": "hello1"},
						map[string]interface{}{"b": "hello2"},
					},
				},
			},
		},
		{ // 15
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`{"a": []}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": make([]interface{}, 0),
				},
			},
		},
		{ // 16
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`{"a": null}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}(nil),
				},
			},
		},
		{ // 17
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`{"a": [null, {"b" : "hello2"}]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						map[string]interface{}(nil),
						map[string]interface{}{"b": "hello2"},
					},
				},
			},
		},
		{ // 18
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.ARRAY,
						FieldType: &ast.ArrayType{
							Type: ast.BIGINT,
						},
					}},
				},
			},
			data: []byte(`{"a": [[50, 60, 70],[66], [77]]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						[]interface{}{int64(50), int64(60), int64(70)},
						[]interface{}{int64(66)},
						[]interface{}{int64(77)},
					},
				},
			},
		},
		{ // 19
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.ARRAY,
						FieldType: &ast.ArrayType{
							Type: ast.BIGINT,
						},
					}},
				},
			},
			data: []byte(`{"a": [null, [66], [77]]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						[]interface{}(nil),
						[]interface{}{int64(66)},
						[]interface{}{int64(77)},
					},
				},
			},
		},
		{ // 20
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": [{"b" : "hello1"}, {"b" : "hello2"}]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						map[string]interface{}{"b": "hello1"},
						map[string]interface{}{"b": "hello2"},
					},
				},
			},
		},
		{ // 21
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.FLOAT,
					}},
				},
			},
			data:   []byte(`{"a": "[\"55\", \"77\"]"}`),
			result: errors.New("error in preprocessor: field a type mismatch: expect array but got [\"55\", \"77\"]"),
		},
		{ // 22
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": [55, 77]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						float64(55),
						float64(77),
					},
				},
			},
		},
		// Rec of complex type
		{ // 23
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "c", FieldType: &ast.RecType{
								StreamFields: []ast.StreamField{
									{Name: "d", FieldType: &ast.BasicType{Type: ast.BIGINT}},
								},
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": {"d": 35.2}}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": map[string]interface{}{
							"d": int64(35),
						},
					},
				},
			},
		},
		{ // 24
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "c", FieldType: &ast.RecType{
								StreamFields: []ast.StreamField{
									{Name: "d", FieldType: &ast.BasicType{Type: ast.BIGINT}},
								},
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": null}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}(nil),
				},
			},
		},
		{ // 25
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "c", FieldType: &ast.ArrayType{
								Type: ast.FLOAT,
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": [35.2, 38.2]}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": []interface{}{
							35.2, 38.2,
						},
					},
				},
			},
		},
		{ // 26
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "c", FieldType: &ast.ArrayType{
								Type: ast.FLOAT,
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": null}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": []interface{}(nil),
					},
				},
			},
		},
		{ // 27
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "c", FieldType: &ast.ArrayType{
								Type: ast.FLOAT,
							}},
						},
					}},
				},
			},
			data:   []byte(`{"a": {"b" : "hello", "c": [null, 35.4]}}`),
			result: errors.New("error in preprocessor: field a type mismatch: field c type mismatch: array element type mismatch: cannot convert <nil>(<nil>) to float64"),
		},
		{ // 28
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"a": {"b" : "hello", "c": {"d": 35.2}}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
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

	defer conf.CloseLogger()
	contextLogger := conf.Log.WithField("rule", "TestPreprocessor_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &Preprocessor{checkSchema: true}
		pp.streamFields = tt.stmt.StreamFields.ToJsonSchema()

		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := pp.Apply(ctx, tuple, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorTime_Apply(t *testing.T) {
	tests := []struct {
		stmt   *ast.StreamStmt
		data   []byte
		result interface{}
	}{
		{ // 0
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.DATETIME}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.DATETIME}},
				},
			},
			data: []byte(`{"abc": "2019-09-19T00:55:15.000Z", "def" : 1568854573431}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": cast.TimeFromUnixMilli(1568854515000),
					"def": cast.TimeFromUnixMilli(1568854573431),
				},
			},
		},
		{ // 1
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`{"abc": "2019-09-19T00:55:15.000Z", "def" : 1568854573431}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": "2019-09-19T00:55:15.000Z",
					"def": float64(1568854573431),
				},
			},
		},
		{ // 2
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.DATETIME}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.DATETIME}},
				},
			},
			data:   []byte(`{"abc": "2019-09-19T00:55:1dd5Z", "def" : 111568854573431}`),
			result: errors.New("error in preprocessor: field abc type mismatch: parsing time \"2019-09-19T00:55:1dd5Z\" as \"2006-01-02T15:04:05.000Z07:00\": cannot parse \"1dd5Z\" as \"05\""),
		},
		{ // 3
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.DATETIME}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.DATETIME}},
				},
				Options: &ast.Options{
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
				"abc": cast.TimeFromUnixMilli(1568894115000),
				"def": cast.TimeFromUnixMilli(1568854573431),
			}},
		},
		// Array type
		{ // 4
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.DATETIME,
					}},
				},
			},
			data: []byte(`{"a": [1568854515123, 1568854573431]}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						cast.TimeFromUnixMilli(1568854515123),
						cast.TimeFromUnixMilli(1568854573431),
					},
				},
			},
		},
		{ // 5
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "c", FieldType: &ast.BasicType{Type: ast.DATETIME}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": 1568854515000}}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": "hello",
						"c": cast.TimeFromUnixMilli(1568854515000),
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer conf.CloseLogger()
	contextLogger := conf.Log.WithField("rule", "TestPreprocessorTime_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &Preprocessor{checkSchema: true}
		pp.streamFields = tt.stmt.StreamFields.ToJsonSchema()
		if tt.stmt.Options != nil {
			pp.timestampFormat = tt.stmt.Options.TIMESTAMP_FORMAT
		}
		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := pp.Apply(ctx, tuple, fv, afv)
			// workaround make sure all the timezone are the same for time vars or the DeepEqual will be false.
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

func convertFields(o ast.StreamFields) []interface{} {
	if o == nil {
		return nil
	}
	fields := make([]interface{}, len(o))
	for i := range o {
		fields[i] = &o[i]
	}
	return fields
}

func TestPreprocessorEventtime_Apply(t *testing.T) {
	tests := []struct {
		stmt   *ast.StreamStmt
		data   []byte
		result interface{}
	}{
		// Basic type
		{ // 0
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{
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
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": int64(1568854515000),
				}, Timestamp: 1568854515000,
			},
		},
		{ // 1
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
				Options: &ast.Options{
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
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": float64(1568854515000),
				}, Timestamp: 1568854515000,
			},
		},
		{ // 2
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BOOLEAN}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					TIMESTAMP:  "abc",
				},
			},
			data:   []byte(`{"abc": true}`),
			result: errors.New("cannot convert timestamp field abc to timestamp with error unsupported type to convert to timestamp true"),
		},
		{ // 3
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.STRINGS}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					TIMESTAMP:  "def",
				},
			},
			data: []byte(`{"abc": 34, "def" : "2019-09-23T02:47:29.754Z", "ghi": 50}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": float64(34),
					"def": "2019-09-23T02:47:29.754Z",
					"ghi": float64(50),
				}, Timestamp: int64(1569206849754),
			},
		},
		{ // 4
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.DATETIME}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.DATETIME}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					TIMESTAMP:  "abc",
				},
			},
			data: []byte(`{"abc": "2019-09-19T00:55:15.000Z", "def" : 1568854573431}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": cast.TimeFromUnixMilli(1568854515000),
					"def": cast.TimeFromUnixMilli(1568854573431),
				}, Timestamp: int64(1568854515000),
			},
		},
		{ // 5
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.STRINGS}},
				},
				Options: &ast.Options{
					DATASOURCE:       "users",
					TIMESTAMP:        "def",
					TIMESTAMP_FORMAT: "yyyy-MM-dd'AT'HH:mm:ss",
				},
			},
			data: []byte(`{"abc": 34, "def" : "2019-09-23AT02:47:29", "ghi": 50}`),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"abc": float64(34),
					"def": "2019-09-23AT02:47:29",
					"ghi": float64(50),
				}, Timestamp: int64(1569206849000),
			},
		},
		{ // 6
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.FLOAT}},
					{Name: "def", FieldType: &ast.BasicType{Type: ast.STRINGS}},
				},
				Options: &ast.Options{
					DATASOURCE:       "users",
					TIMESTAMP:        "def",
					TIMESTAMP_FORMAT: "yyyy-MM-ddaHH:mm:ss",
				},
			},
			data:   []byte(`{"abc": 34, "def" : "2019-09-23AT02:47:29", "ghi": 50}`),
			result: errors.New("cannot convert timestamp field def to timestamp with error parsing time \"2019-09-23AT02:47:29\" as \"2006-01-02PM15:04:05\": cannot parse \"AT02:47:29\" as \"PM\""),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer conf.CloseLogger()
	contextLogger := conf.Log.WithField("rule", "TestPreprocessorEventtime_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {

		pp := &Preprocessor{
			checkSchema: true,
			defaultFieldProcessor: defaultFieldProcessor{
				streamFields:    tt.stmt.StreamFields.ToJsonSchema(),
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
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := pp.Apply(ctx, tuple, fv, afv)
			// workaround make sure all the timezone are the same for time vars or the DeepEqual will be false.
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
		stmt   *ast.StreamStmt
		data   []byte
		result interface{}
	}{
		{
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
			},
			data:   []byte(`{"abc": "dafsad"}`),
			result: errors.New("error in preprocessor: field abc type mismatch: cannot convert string(dafsad) to int64"),
		}, {
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
						},
					}},
				},
			},
			data:   []byte(`{"a": {"d" : "hello"}}`),
			result: errors.New("error in preprocessor: field a type mismatch: field b is not found"),
		}, {
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "abc", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{
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
			result: errors.New("error in preprocessor: field abc type mismatch: cannot convert string(not a time) to int64"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer conf.CloseLogger()
	contextLogger := conf.Log.WithField("rule", "TestPreprocessorError")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {

		pp := &Preprocessor{checkSchema: true}
		pp.streamFields = tt.stmt.StreamFields.ToJsonSchema()
		dm := make(map[string]interface{})
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := pp.Apply(ctx, tuple, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tuple, tt.result, result)
			}
		}

	}
}

func TestPreprocessorForBinary(t *testing.T) {
	docsFolder, err := conf.GetLoc("docs/")
	if err != nil {
		t.Errorf("Cannot find docs folder: %v", err)
	}
	image, err := os.ReadFile(path.Join(docsFolder, "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	tests := []struct {
		stmt   *ast.StreamStmt
		data   []byte
		result interface{}
	}{
		{ // 0
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "b", FieldType: &ast.BasicType{Type: ast.BYTEA}},
						},
					}},
				},
			},
			data: []byte(fmt.Sprintf(`{"a": {"b" : "%s"}}`, b64img)),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": map[string]interface{}{
						"b": image,
					},
				},
			},
		},
		{ // 1
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.BYTEA,
					}},
				},
			},
			data: []byte(fmt.Sprintf(`{"a": ["%s"]}`, b64img)),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						image,
					},
				},
			},
		},
		{
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "b", FieldType: &ast.BasicType{Type: ast.BYTEA}},
							},
						},
					}},
				},
			},
			data: []byte(fmt.Sprintf(`{"a": [{"b":"%s"}]}`, b64img)),
			result: &xsql.Tuple{
				Message: xsql.Message{
					"a": []interface{}{
						map[string]interface{}{"b": image},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer conf.CloseLogger()
	contextLogger := conf.Log.WithField("rule", "TestPreprocessorForBinary")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &Preprocessor{checkSchema: true}
		pp.streamFields = tt.stmt.StreamFields.ToJsonSchema()
		format := message.FormatJson
		ccc, _ := converter.GetOrCreateConverter(&ast.Options{FORMAT: format})
		nCtx := context.WithValue(ctx, context.DecodeKey, ccc)
		if dm, e := nCtx.Decode(tt.data); e != nil {
			log.Fatal(e)
			return
		} else {
			tuple := &xsql.Tuple{Message: dm}
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := pp.Apply(ctx, tuple, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. %q\n\nresult mismatch", i, tuple)
			}
		}

	}
}
