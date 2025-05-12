// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

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
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		f := NewFastJsonConverter(tc.schema, nil)
		v, err := f.Decode(ctx, tc.payload)
		require.NoError(t, err)
		require.Equal(t, v, tc.require)
	}

	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		f := NewFastJsonConverter(tc.schema, nil)
		v, err := f.Decode(ctx, arrayPayload)
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
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		f := NewFastJsonConverter(tc.schema, nil)
		_, err := f.Decode(ctx, tc.payload)
		require.Error(t, err)
		require.Equal(t, err.Error(), tc.err.Error())
	}
}

func TestFastJsonEncode(t *testing.T) {
	a := make(map[string]int)
	a["a"] = 1
	ctx := mockContext.NewMockContext("test", "op1")
	f := NewFastJsonConverter(nil, nil)
	v, err := f.Encode(ctx, a)
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
	ctx := mockContext.NewMockContext("test", "op1")
	f := NewFastJsonConverter(schema, nil)
	v, err := f.Decode(ctx, payload)
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
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		f := NewFastJsonConverter(tc.schema, nil)
		v, err := f.Decode(ctx, arrayPayload)
		require.NoError(t, err)
		require.Equal(t, v, arrayRequire)
	}
	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		f := NewFastJsonConverter(tc.schema, nil)
		v, err := f.Decode(ctx, arrayPayload)
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
	ctx := mockContext.NewMockContext("test", "op1")
	f := NewFastJsonConverter(schema, nil)
	v, err := f.Decode(ctx, []byte(payload))
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
	f = NewFastJsonConverter(schema, nil)
	v, err = f.Decode(ctx, []byte(payload))
	require.NoError(t, err)
	require.Equal(t, v, map[string]interface{}{
		"a": []interface{}{[]byte(origin)},
	})
}

func TestSchemaless(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"a": nil,
	}
	f := NewFastJsonConverter(originSchema, nil)
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
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		bs, _ := json.Marshal(tc.data)
		v, err := f.Decode(ctx, bs)
		require.NoError(t, err)
		require.Equal(t, tc.expect, v)
	}
}

func TestIssue(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"results": nil,
	}
	f := NewFastJsonConverter(originSchema, nil)
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
	m, err := f.Decode(context.Background(), []byte(data))
	require.NoError(t, err)
	expected := make(map[string]interface{})
	json.Unmarshal([]byte(data), &expected)
	require.Equal(t, expected, m)

	schmema2 := map[string]*ast.JsonStreamField{
		"others": nil,
	}
	f2 := NewFastJsonConverter(schmema2, nil)
	m, err = f2.Decode(context.Background(), []byte(data))
	require.NoError(t, err)
	require.Len(t, m, 0)
}

func TestDecodeField(t *testing.T) {
	testcases := []struct {
		name    string
		payload []byte
		result  any
		err     string
	}{
		{
			name:    "normal",
			payload: []byte(`{"id":1, "value":"vv"}`),
			result:  1.0,
		},
		{
			name:    "empty",
			payload: []byte(`{"a":[true]}`),
			result:  nil,
		},
		{
			name:    "composite",
			payload: []byte(`{"id":[true]}`),
			result:  nil,
		},
		{
			name:    "invalid",
			payload: []byte(`{"a":1`),
			err:     "cannot parse JSON: cannot parse object: unexpected end of object; unparsed tail: \"\"",
		},
		{
			name:    "string",
			payload: []byte(`{"id":"1", "value":"vv"}`),
			result:  `"1"`,
		},
		{
			name:    "bool",
			payload: []byte(`{"id":false, "value":"vv"}`),
			result:  false,
		},
		{
			name:    "not obj",
			payload: []byte(`[{"id":false, "value":"vv"}]`),
			result:  nil,
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	f := NewFastJsonConverter(nil, nil)
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			field, err := f.DecodeField(ctx, tc.payload, "id")
			if tc.err != "" {
				assert.EqualError(t, err, tc.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.result, field)
			}
		})
	}
}

func TestIssue3441(t *testing.T) {
	originSchema := map[string]*ast.JsonStreamField{
		"id": nil,
	}
	f := NewFastJsonConverter(originSchema, map[string]any{"useInt64ForWholeNumber": true})
	data := `{"id":1795292668348461056}`
	ctx := mockContext.NewMockContext("test", "op1")
	m, err := f.Decode(ctx, []byte(data))
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"id": int64(1795292668348461056)}, m)

	data = `{"id":17952926683484.44}`
	m, err = f.Decode(ctx, []byte(data))
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"id": 17952926683484.44}, m)
}

func TestSliceDecode(t *testing.T) {
	origin := "123"
	encode := base64.StdEncoding.EncodeToString([]byte(origin))
	testcases := []struct {
		name    string
		schema  map[string]*ast.JsonStreamField
		payload []byte
		require model.SliceVal
	}{
		{
			name:    "normal",
			payload: []byte(`{"a":1}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "bigint",
					HasIndex: true,
					Index:    0,
				},
			},
			require: model.SliceVal{
				int64(1),
			},
		},
		{
			name:    "float",
			payload: []byte(`{"a":1}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "float",
					HasIndex: true,
					Index:    0,
				},
			},
			require: model.SliceVal{
				float64(1),
			},
		},
		{
			name:    "string",
			payload: []byte(`{"a":"a"}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "string",
					HasIndex: true,
				},
			},
			require: model.SliceVal{
				"a",
			},
		},
		{
			payload: []byte(fmt.Sprintf(`{"a":"%v"}`, encode)),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "bytea",
					HasIndex: true,
				},
			},
			require: model.SliceVal{
				[]byte(origin),
			},
		},
		{
			payload: []byte(`{"a":true}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "boolean",
					HasIndex: true,
				},
			},
			require: model.SliceVal{
				true,
			},
		},
		{
			payload: []byte(`{"a":123}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "datetime",
					HasIndex: true,
				},
			},
			require: model.SliceVal{
				float64(123),
			},
		},
		{
			payload: []byte(`{"a":"123"}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type:     "datetime",
					HasIndex: true,
				},
			},
			require: model.SliceVal{
				"123",
			},
		},
		// not supported yet
		//{
		//	name:    "embed",
		//	payload: []byte(`{"a":{"b":1}}`),
		//	schema: map[string]*ast.JsonStreamField{
		//		"a": {
		//			HasIndex: true,
		//			Index:    0,
		//		},
		//	},
		//	require: model.SliceVal{
		//		map[string]any{
		//			"b": int64(1),
		//		},
		//	},
		//},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFastJsonConverter(tc.schema, nil)
			v, err := f.Decode(ctx, tc.payload)
			require.NoError(t, err)
			require.Equal(t, v, tc.require)
		})
	}
}

func TestSliceEncode(t *testing.T) {
	schema := map[string]*ast.JsonStreamField{
		"a": {
			HasIndex: true,
			Index:    1,
		},
		"b": {
			HasIndex: true,
			Index:    0,
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	f := NewFastJsonConverter(nil, nil)

	data := model.SliceVal{
		2, 1,
	}
	f.ResetSchema(schema)
	v, err := f.Encode(ctx, data)
	require.NoError(t, err)
	require.Equal(t, string(v), "{\"a\":1,\"b\":2}")
	list := []model.SliceVal{
		{4, 2},
		{5, 6},
	}
	v, err = f.Encode(ctx, list)
	require.NoError(t, err)
	require.Equal(t, string(v), "[{\"a\":2,\"b\":4},{\"a\":6,\"b\":5}]")
}
