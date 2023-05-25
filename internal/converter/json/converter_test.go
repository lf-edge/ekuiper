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
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/pkg/ast"
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
	testcases := []struct {
		schema  map[string]*ast.JsonStreamField
		payload []byte
		require map[string]interface{}
	}{
		{
			payload: []byte(`{"a":1}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			},
			require: map[string]interface{}{
				"a": float64(1),
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
			payload: []byte(`{"a":"a"}`),
			schema: map[string]*ast.JsonStreamField{
				"a": {
					Type: "bytea",
				},
			},
			require: map[string]interface{}{
				"a": "a",
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
					"b": float64(1),
				},
			},
		},
	}
	for _, tc := range testcases {
		v, err := fastConverter.DecodeWithSchema(tc.payload, tc.schema)
		require.NoError(t, err)
		require.Equal(t, v, tc.require)
	}

	for _, tc := range testcases {
		arrayPayload := []byte(fmt.Sprintf("[%s]", string(tc.payload)))
		arrayRequire := []map[string]interface{}{
			tc.require,
		}
		v, err := fastConverter.DecodeWithSchema(arrayPayload, tc.schema)
		require.NoError(t, err)
		require.Equal(t, v, arrayRequire)
	}
}
