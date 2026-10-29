// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestLinesDecode(t *testing.T) {
	testcases := []struct {
		name    string
		schema  map[string]*ast.JsonStreamField
		props   map[string]any
		payload string
		result  []map[string]any
	}{
		{
			name:    "lines",
			payload: "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n",
			result: []map[string]any{
				{"a": float64(1), "b": "x"},
				{"a": float64(2), "b": "y"},
			},
		},
		{
			name:    "no trailing newline",
			payload: "{\"a\":1}\n{\"a\":2}",
			result: []map[string]any{
				{"a": float64(1)},
				{"a": float64(2)},
			},
		},
		{
			name:    "crlf and blank lines",
			payload: "\r\n{\"a\":1}\r\n\r\n  {\"a\":2}\r\n",
			result: []map[string]any{
				{"a": float64(1)},
				{"a": float64(2)},
			},
		},
		{
			name:    "concatenated",
			payload: `{"a":1}{"a":2} {"a":3}`,
			result: []map[string]any{
				{"a": float64(1)},
				{"a": float64(2)},
				{"a": float64(3)},
			},
		},
		{
			name:    "single record",
			payload: `{"a":{"b":[1,2]}}`,
			result: []map[string]any{
				{"a": map[string]any{"b": []any{float64(1), float64(2)}}},
			},
		},
		{
			name:    "empty",
			payload: "",
			result:  nil,
		},
		{
			name:    "whitespace only",
			payload: "\n\n",
			result:  nil,
		},
		{
			name: "schema",
			schema: map[string]*ast.JsonStreamField{
				"a": {Type: "bigint"},
				"b": {Type: "string"},
			},
			payload: "{\"a\":1,\"b\":\"x\",\"c\":true}\n{\"a\":2,\"c\":false}",
			result: []map[string]any{
				{"a": int64(1), "b": "x"},
				{"a": int64(2)},
			},
		},
		{
			name:    "props",
			props:   map[string]any{"useInt64ForWholeNumber": true, "colAliasMapping": map[string]string{"a": "aa"}},
			payload: "{\"a\":1,\"b\":1.5}\n{\"a\":2}",
			result: []map[string]any{
				{"aa": int64(1), "b": 1.5},
				{"aa": int64(2)},
			},
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewLinesConverter(tc.schema, tc.props)
			v, err := c.Decode(ctx, []byte(tc.payload))
			require.NoError(t, err)
			require.IsType(t, []map[string]any{}, v)
			if tc.result == nil {
				require.Empty(t, v)
			} else {
				require.Equal(t, tc.result, v)
			}
		})
	}
}

func TestLinesDecodeError(t *testing.T) {
	testcases := []struct {
		name    string
		schema  map[string]*ast.JsonStreamField
		payload string
		err     string
	}{
		{
			name:    "array record",
			payload: "{\"a\":1}\n[{\"a\":2}]",
			err:     "JSON Lines record 2 must be an object but got array",
		},
		{
			name:    "scalar record",
			payload: "1",
			err:     "JSON Lines record 1 must be an object but got number",
		},
		{
			name:    "malformed",
			payload: "{\"a\":1}\n{\"a\":",
			err:     "JSON Lines record 2: cannot parse object",
		},
		{
			name: "schema mismatch",
			schema: map[string]*ast.JsonStreamField{
				"a": {Type: "bigint"},
			},
			payload: "{\"a\":1}\n{\"a\":\"x\"}",
			err:     "JSON Lines record 2: a has wrong type:string, expect:bigint",
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewLinesConverter(tc.schema, nil)
			_, err := c.Decode(ctx, []byte(tc.payload))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.err)
		})
	}
}

func TestLinesSliceDecode(t *testing.T) {
	schema := map[string]*ast.JsonStreamField{
		"a": {Type: "bigint", HasIndex: true, Index: 0},
		"b": {Type: "string", HasIndex: true, Index: 1},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	c := NewLinesConverter(schema, nil)
	v, err := c.Decode(ctx, []byte("{\"a\":1,\"b\":\"x\",\"c\":1}\n{\"b\":\"y\"}"))
	require.NoError(t, err)
	require.Equal(t, []model.SliceVal{{int64(1), "x"}, {nil, "y"}}, v)
}

func TestLinesResetSchema(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	c := NewLinesConverter(map[string]*ast.JsonStreamField{"a": nil}, nil)
	v, err := c.Decode(ctx, []byte(`{"a":1,"b":2}`))
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"a": float64(1)}}, v)
	c.ResetSchema(map[string]*ast.JsonStreamField{"a": nil, "b": nil})
	v, err = c.Decode(ctx, []byte(`{"a":1,"b":2}`))
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"a": float64(1), "b": float64(2)}}, v)
}

func TestLinesEncode(t *testing.T) {
	testcases := []struct {
		name   string
		data   any
		result string
	}{
		{
			name:   "map",
			data:   map[string]any{"a": 1, "b": "x"},
			result: `{"a":1,"b":"x"}`,
		},
		{
			name:   "maps",
			data:   []map[string]any{{"a": 1}, {"a": 2}},
			result: "{\"a\":1}\n{\"a\":2}",
		},
		{
			name:   "any list",
			data:   []any{map[string]any{"a": 1}, map[string]any{"a": 2}},
			result: "{\"a\":1}\n{\"a\":2}",
		},
		{
			name:   "empty list",
			data:   []map[string]any{},
			result: "",
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	c := NewLinesConverter(nil, nil)
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := c.Encode(ctx, tc.data)
			require.NoError(t, err)
			require.Equal(t, tc.result, string(v))
		})
	}
}

func TestLinesSliceEncode(t *testing.T) {
	schema := map[string]*ast.JsonStreamField{
		"a": {HasIndex: true, Index: 1},
		"b": {HasIndex: true, Index: 0},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	c := NewLinesConverter(schema, nil)
	v, err := c.Encode(ctx, model.SliceVal{2, 1})
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"b":2}`, string(v))
	v, err = c.Encode(ctx, []model.SliceVal{{4, 2}, {5, 6}})
	require.NoError(t, err)
	require.Equal(t, "{\"a\":2,\"b\":4}\n{\"a\":6,\"b\":5}", string(v))
}

func TestLinesRoundTrip(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	c := NewLinesConverter(nil, nil)
	data := []map[string]any{
		{"a": float64(1), "s": "line\nbreak"},
		{"a": float64(2), "o": map[string]any{"b": []any{"x"}}},
	}
	b, err := c.Encode(ctx, data)
	require.NoError(t, err)
	require.Len(t, strings.Split(string(b), "\n"), 2)
	v, err := c.Decode(ctx, b)
	require.NoError(t, err)
	require.Equal(t, data, v)
}

func TestLinesWrite(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	w := NewLinesConverter(nil, nil)
	for i := 0; i < 2; i++ {
		require.NoError(t, w.New(ctx))
		require.NoError(t, w.Write(ctx, map[string]any{"id": 1}))
		require.NoError(t, w.Write(ctx, []map[string]any{{"id": 2}, {"id": 3}}))
		require.NoError(t, w.Write(ctx, []map[string]any{}))
		require.NoError(t, w.WriteRaw(ctx, []byte(`{"id":4}`)))
		r, err := w.Flush(ctx)
		require.NoError(t, err)
		require.Equal(t, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}", string(r))
	}
}

func TestLinesSliceWrite(t *testing.T) {
	schema := map[string]*ast.JsonStreamField{
		"id":   {HasIndex: true, Index: 0},
		"name": {HasIndex: true, Index: 1},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	w := NewLinesConverter(schema, nil)
	require.NoError(t, w.New(ctx))
	require.NoError(t, w.Write(ctx, model.SliceVal{int64(1), "a"}))
	require.NoError(t, w.Write(ctx, model.SliceVal{int64(2), nil}))
	r, err := w.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, "{\"id\":1,\"name\":\"a\"}\n{\"id\":2}", string(r))
}

func TestLinesWriterFlushOwnership(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	w := NewLinesConverter(nil, nil)
	require.NoError(t, w.New(ctx))
	require.NoError(t, w.WriteRaw(ctx, []byte(`{"id":1}`)))
	first, err := w.Flush(ctx)
	require.NoError(t, err)
	firstSnapshot := append([]byte(nil), first...)

	require.NoError(t, w.New(ctx))
	require.NoError(t, w.WriteRaw(ctx, []byte(`{"id":2}`)))
	_, err = w.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, firstSnapshot, first)
}
