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

package json

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/converter/merge"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

func TestMessageDecode(t *testing.T) {
	image, err := os.ReadFile(path.Join("../../../docs", "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	tests := []struct {
		payload []byte
		format  string
		result  map[string]interface{}
		results []interface{}
	}{
		{
			payload: []byte(fmt.Sprintf(`{"format":"jpg","content":"%s"}`, b64img)),
			format:  "json",
			result: map[string]interface{}{
				"format":  "jpg",
				"content": b64img,
			},
		},
		{
			payload: []byte(`[{"a":1},{"a":2}]`),
			format:  "json",
			results: []interface{}{
				map[string]interface{}{
					"a": float64(1),
				},
				map[string]interface{}{
					"a": float64(2),
				},
			},
		},
	}
	conv, _ := GetConverter()
	for i, tt := range tests {
		result, err := conv.Decode(tt.payload)
		if err != nil {
			t.Errorf("%d decode error: %v", i, err)
		}
		if len(tt.results) > 0 {
			if !reflect.DeepEqual(tt.results, result) {
				t.Errorf("%d result mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, result)
			}
		} else {
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d result mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, result)
			}
		}
	}
}

func TestFastJsonConverterWithSchema(t *testing.T) {
	origin := "123"
	encode := base64.StdEncoding.EncodeToString([]byte(origin))
	testcases := []struct {
		schema  map[string]*ast.JsonStreamField
		payload []byte
		require map[string]interface{}
	}{
		{
			payload: []byte(`{"a":["true"]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "boolean",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{true},
			},
		},
		{
			payload: []byte(`{"a":[true]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "boolean",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{true},
			},
		},
		{
			payload: []byte(`{"a":1}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			require: map[string]interface{}{
				"a": int64(1),
			},
		},
		{
			payload: []byte(`{"a":1}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "float",
				},
			},
			require: map[string]interface{}{
				"a": float64(1),
			},
		},
		{
			payload: []byte(`{"a":"a"}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "string",
				},
			},
			require: map[string]interface{}{
				"a": "a",
			},
		},
		{
			payload: []byte(fmt.Sprintf(`{"a":"%v"}`, encode)),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bytea",
				},
			},
			require: map[string]interface{}{
				"a": []byte(origin),
			},
		},
		{
			payload: []byte(`{"a":true}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "boolean",
				},
			},
			require: map[string]interface{}{
				"a": true,
			},
		},
		{
			payload: []byte(`{"a":123}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "datetime",
				},
			},
			require: map[string]interface{}{
				"a": float64(123),
			},
		},
		{
			payload: []byte(`{"a":"123"}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "datetime",
				},
			},
			require: map[string]interface{}{
				"a": "123",
			},
		},
		{
			payload: []byte(`{"a":{"b":1}}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
					},
				},
			},
			require: map[string]interface{}{
				"a": map[string]interface{}{
					"b": int64(1),
				},
			},
		},
	}
	for _, tc := range testcases {
		f := NewFastJsonConverter("", "", tc.schema, false, false)
		v, err := f.Decode(tc.payload)
		require.NoError(t, err)
		require.Equal(t, v, tc.require)
	}

	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		f := NewFastJsonConverter("", "", tc.schema, false, false)
		v, err := f.Decode(arrayPayload)
		require.NoError(t, err)
		require.Equal(t, v, arrayRequire)
	}
}

func TestFastJsonConverterWithSchemaError(t *testing.T) {
	testcases := []struct {
		schema  map[string]*ast.JsonStreamField
		payload []byte
		err     error
	}{
		{
			payload: []byte(`{123}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			err: fmt.Errorf(`cannot parse JSON: cannot parse object: cannot find opening '"" for object key; unparsed tail: "123}"`),
		},
		{
			payload: []byte(`123`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			err: fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported"),
		},
		{
			payload: []byte(`{"a":{"b":1}}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			err: fmt.Errorf("a has wrong type:object, expect:bigint"),
		},
		{
			payload: []byte(`{"a":{"b":1}}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "string",
				},
			},
			err: fmt.Errorf("a has wrong type:object, expect:string"),
		},
		{
			payload: []byte(`{"a":123}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
				},
			},
			err: fmt.Errorf("a has wrong type:number, expect:array"),
		},
		{
			payload: []byte(`{"a":123}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
				},
			},
			err: fmt.Errorf("a has wrong type:number, expect:struct"),
		},
		{
			payload: []byte(`{"a":{"b":1}}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "boolean",
				},
			},
			err: fmt.Errorf("a has wrong type:object, expect:boolean"),
		},
		{
			payload: []byte(`{"a":true}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "datetime",
				},
			},
			err: fmt.Errorf("a has wrong type:true, expect:datetime"),
		},
		{
			payload: []byte(`{"a":[{"b":1}]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "bigint",
					},
				},
			},
			err: fmt.Errorf("array has wrong type:object, expect:bigint"),
		},
		{
			payload: []byte(`{"a":[{"b":1}]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "string",
					},
				},
			},
			err: fmt.Errorf("array has wrong type:object, expect:string"),
		},
		{
			payload: []byte(`{"a":[123]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "array",
					},
				},
			},
			err: fmt.Errorf("array has wrong type:number, expect:array"),
		},
		{
			payload: []byte(`{"a":[123]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "struct",
					},
				},
			},
			err: fmt.Errorf("array has wrong type:number, expect:struct"),
		},
		{
			payload: []byte(`{"a":[{"b":1}]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "boolean",
					},
				},
			},
			err: fmt.Errorf("array has wrong type:object, expect:boolean"),
		},
		{
			payload: []byte(`{"a":[true]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "datetime",
					},
				},
			},
			err: fmt.Errorf("array has wrong type:true, expect:datetime"),
		},
	}

	for _, tc := range testcases {
		f := NewFastJsonConverter("", "", tc.schema, false, false)
		_, err := f.Decode(tc.payload)
		require.Error(t, err)
		require.Equal(t, err.Error(), tc.err.Error())
	}
}

func TestFastJsonEncode(t *testing.T) {
	a := make(map[string]int)
	a["a"] = 1
	f := NewFastJsonConverter("", "", nil, false, false)
	v, err := f.Encode(a)
	require.NoError(t, err)
	require.Equal(t, v, []byte(`{"a":1}`))
}

func TestArrayWithArray(t *testing.T) {
	payload := []byte(`{
    "a":[
        [
            {
                "c":1
            }
        ]
    ]
}`)
	schema := map[string]*ast.JsonStreamField{
		"a": {
			Type: "array",
			Items: &ast.JsonStreamField{
				Type: "array",
				Items: &ast.JsonStreamField{
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"c": {
							Type: "bigint",
						},
					},
				},
			},
		},
	}
	f := NewFastJsonConverter("", "", schema, false, false)
	v, err := f.Decode(payload)
	require.NoError(t, err)
	require.Equal(t, v, map[string]interface{}{
		"a": []interface{}{
			[]interface{}{
				map[string]interface{}{
					"c": int64(1),
				},
			},
		},
	})
}

func TestTypeNull(t *testing.T) {
	testcases := []struct {
		schema  map[string]*ast.JsonStreamField
		payload []byte
		require map[string]interface{}
	}{
		{
			payload: []byte(`{"a":[null]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "bytea",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{nil},
			},
		},
		{
			payload: []byte(`{"a":[null]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "string",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{nil},
			},
		},
		{
			payload: []byte(`{"a":[null]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "float",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{nil},
			},
		},
		{
			payload: []byte(`{"a":[null]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "bigint",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{nil},
			},
		},
		{
			payload: []byte(`{"a":[null]}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "boolean",
					},
				},
			},
			require: map[string]interface{}{
				"a": []interface{}{nil},
			},
		},
		{
			payload: []byte(`{"a":null}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			require: map[string]interface{}{
				"a": nil,
			},
		},
		{
			payload: []byte(`{"a":null}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "float",
				},
			},
			require: map[string]interface{}{
				"a": nil,
			},
		},
		{
			payload: []byte(`{"a":null}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "string",
				},
			},
			require: map[string]interface{}{
				"a": nil,
			},
		},
		{
			payload: []byte(`{"a":null}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bytea",
				},
			},
			require: map[string]interface{}{
				"a": nil,
			},
		},
		{
			payload: []byte(`{"a":null}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "boolean",
				},
			},
			require: map[string]interface{}{
				"a": nil,
			},
		},
		{
			payload: []byte(`{"a":null}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "datetime",
				},
			},
			require: map[string]interface{}{
				"a": nil,
			},
		},
		{
			payload: []byte(`{"a":{"b":null}}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
					},
				},
			},
			require: map[string]interface{}{
				"a": map[string]interface{}{
					"b": nil,
				},
			},
		},
	}
	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		f := NewFastJsonConverter("", "", tc.schema, false, false)
		v, err := f.Decode(arrayPayload)
		require.NoError(t, err)
		require.Equal(t, v, arrayRequire)
	}
	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		f := NewFastJsonConverter("", "", tc.schema, false, false)
		v, err := f.Decode(arrayPayload)
		require.NoError(t, err)
		require.Equal(t, v, arrayRequire)
	}
}

func TestConvertBytea(t *testing.T) {
	origin := "123"
	encode := base64.StdEncoding.EncodeToString([]byte(origin))
	payload := fmt.Sprintf(`{"a":"%s"}`, encode)
	schema := map[string]*ast.JsonStreamField{
		"a": {
			Type: "bytea",
		},
	}
	f := NewFastJsonConverter("", "", schema, false, false)
	v, err := f.Decode([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, v, map[string]interface{}{
		"a": []byte(origin),
	})

	payload = fmt.Sprintf(`{"a":["%s"]}`, encode)
	schema = map[string]*ast.JsonStreamField{
		"a": {
			Type: "array",
			Items: &ast.JsonStreamField{
				Type: "bytea",
			},
		},
	}
	f = NewFastJsonConverter("", "", schema, false, false)
	v, err = f.Decode([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, v, map[string]interface{}{
		"a": []interface{}{[]byte(origin)},
	})
}

func TestMergeSchema(t *testing.T) {
	testcases := []struct {
		originSchema map[string]*ast.JsonStreamField
		newSchema    map[string]*ast.JsonStreamField
		resultSchema map[string]*ast.JsonStreamField
		err          error
	}{
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
			newSchema: map[string]*ast.JsonStreamField{
				"b": nil,
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": nil,
				"b": nil,
			},
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": nil,
			},
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "struct",
						Properties: map[string]*ast.JsonStreamField{
							"b": {
								Type: "bigint",
							},
						},
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "struct",
						Properties: map[string]*ast.JsonStreamField{
							"b": {
								Type: "string",
							},
						},
					},
				},
			},
			err: errors.New("column field type b between current[bigint] and new[string] are not equal"),
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "bigint",
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "array",
					Items: &ast.JsonStreamField{
						Type: "string",
					},
				},
			},
			err: errors.New("array column field type a between current[bigint] and new[string] are not equal"),
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "string",
						},
					},
				},
			},
			err: errors.New("column field type b between current[bigint] and new[string] are not equal"),
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "string",
				},
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
				"b": {
					Type: "string",
				},
			},
		},
		{
			originSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
					},
				},
			},
			newSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"c": {
							Type: "string",
						},
					},
				},
			},
			resultSchema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "struct",
					Properties: map[string]*ast.JsonStreamField{
						"b": {
							Type: "bigint",
						},
						"c": {
							Type: "string",
						},
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		f := NewFastJsonConverter("1", "", tc.originSchema, false, false)
		err := f.MergeSchema("2", "", tc.newSchema, false)
		if tc.err == nil {
			require.NoError(t, err)
			require.Equal(t, tc.resultSchema, f.schema)
		} else {
			require.Equal(t, tc.err, err)
		}
	}
}

func TestMergeWildcardSchema(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"a": {
			Type: "bigint",
		},
	}
	f := NewFastJsonConverter("1", "", originSchema, false, false)
	require.NoError(t, f.MergeSchema("2", "", nil, true))
	newSchema := map[string]*ast.JsonStreamField{
		"b": {
			Type: "bigint",
		},
	}
	require.NoError(t, f.MergeSchema("3", "", newSchema, false))
	data := map[string]interface{}{
		"a": float64(1),
		"b": float64(2),
		"c": float64(3),
	}
	bs, _ := json.Marshal(data)
	d, err := f.Decode(bs)
	require.NoError(t, err)
	require.Equal(t, data, d)
	require.NoError(t, f.DetachSchema("2"))
	d, err = f.Decode(bs)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"a": int64(1),
		"b": int64(2),
	}, d)
}

func TestSchemaless(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"a": nil,
	}
	f := NewFastJsonConverter("1", "", originSchema, false, true)
	testcases := []struct {
		data   map[string]interface{}
		expect map[string]interface{}
	}{
		{
			data: map[string]interface{}{
				"a": float64(1),
				"b": float64(2),
			},
			expect: map[string]interface{}{
				"a": float64(1),
			},
		},

		{
			data: map[string]interface{}{
				"a": "123",
				"b": "123",
			},
			expect: map[string]interface{}{
				"a": "123",
			},
		},
		{
			data: map[string]interface{}{
				"a": map[string]interface{}{
					"b": float64(1),
				},
				"b": 123,
			},
			expect: map[string]interface{}{
				"a": map[string]interface{}{
					"b": float64(1),
				},
			},
		},
	}
	for _, tc := range testcases {
		bs, _ := json.Marshal(tc.data)
		v, err := f.Decode(bs)
		require.NoError(t, err)
		require.Equal(t, tc.expect, v)
	}
}

func TestJsonError(t *testing.T) {
	_, err := converter.Decode(nil)
	require.Error(t, err)
	errWithCode, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.CovnerterErr, errWithCode.Code())
	// fastjson
	c := NewFastJsonConverter("", "", nil, false, true)
	_, err = c.Decode(nil)
	require.Error(t, err)
	errWithCode, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.CovnerterErr, errWithCode.Code())
}

func TestAttachDetachSchema(t *testing.T) {
	f := NewFastJsonConverter("rule1", "demo", nil, true, true)
	err := f.MergeSchema("rule2", "demo", map[string]*ast.JsonStreamField{
		"a": nil,
	}, false)
	require.NoError(t, err)
	r := merge.GetRuleSchema("rule1")
	er := merge.RuleSchemaResponse{
		Schema: map[string]map[string]*ast.JsonStreamField{
			"demo": nil,
		},
		Wildcard: map[string]bool{
			"demo": true,
		},
	}
	require.Equal(t, r, er)
	r = merge.GetRuleSchema("rule2")
	require.Equal(t, r, er)
	// detach rule
	require.NoError(t, f.DetachSchema("rule1"))
	r = merge.GetRuleSchema("rule2")
	er = merge.RuleSchemaResponse{
		Schema: map[string]map[string]*ast.JsonStreamField{
			"demo": {
				"a": nil,
			},
		},
		Wildcard: map[string]bool{
			"demo": false,
		},
	}
	require.Equal(t, r, er)
}

func TestIssue(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"results": nil,
	}
	f := NewFastJsonConverter("1", "2", originSchema, false, true)
	data := `{
    "results": [
        {
            "location": {
                "id": "WTMKQ069CCJ7",
                "name": "杭州",
                "country": "CN",
                "path": "杭州,杭州,浙江,中国",
                "timezone": "Asia/Shanghai",
                "timezone_offset": "+08:00"
            },
            "now": {
                "text": "多云",
                "code": "4",
                "temperature": "31",
                "feels_like": "36",
                "pressure": "997",
                "humidity": "58",
                "visibility": "9.7",
                "wind_direction": "东",
                "wind_direction_degree": "87",
                "wind_speed": "14.0",
                "wind_scale": "3",
                "clouds": "100",
                "dew_point": ""
            },
            "last_update": "2024-06-13T16:37:50+08:00"
        }
    ]
}`
	m, err := f.Decode([]byte(data))
	require.NoError(t, err)
	expected := make(map[string]interface{})
	json.Unmarshal([]byte(data), &expected)
	require.Equal(t, expected, m)

	schmema2 := map[string]*ast.JsonStreamField{
		"others": nil,
	}
	f2 := NewFastJsonConverter("1", "2", schmema2, false, true)
	m, err = f2.Decode([]byte(data))
	require.NoError(t, err)
	require.Len(t, m, 0)
}
