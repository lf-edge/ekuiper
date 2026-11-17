// Copyright 2025 EMQ Technologies Co., Ltd.
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

//go:build parquet || full

package parquet

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestBuildParquetSchema(t *testing.T) {
	data := make(map[string]interface{})
	data["k1"] = "v1"
	data["k2"] = int64(1)
	data["k3"] = float64(3)
	data["k4"] = true
	data["k5"] = []interface{}{int64(1)}
	data["k6"] = map[string]interface{}{
		"k7": "v8",
	}
	ctx := mockContext.NewMockContext("test", "test")
	p := &ParquetWriter{}
	content, err := p.Encode(ctx, data)
	require.NoError(t, err)
	require.NotNil(t, content)
	// The low-level reader exposes Parquet's three-level LIST layout.
	data["k5"] = map[string]any{"list": []any{map[string]any{"element": int64(1)}}}
	require.Equal(t, []map[string]any{data}, readParquetRows(t, content))
}

func readParquetRows(t *testing.T, content []byte) []map[string]any {
	t.Helper()
	file, err := parquet.OpenFile(bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)
	var result []map[string]any
	for _, group := range file.RowGroups() {
		rows := group.Rows()
		for range group.NumRows() {
			values := make([]parquet.Row, 1)
			n, err := rows.ReadRows(values)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
			}
			require.Equal(t, 1, n)
			actual := make(map[string]any)
			require.NoError(t, file.Schema().Reconstruct(&actual, values[0]))
			result = append(result, actual)
		}
		require.NoError(t, rows.Close())
	}
	require.Equal(t, file.NumRows(), int64(len(result)))
	return result
}

func TestEncode(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "parquet")
	first := map[string]any{"id": int64(1), "value": 3.141592653589793, "name": "first", "active": true}
	second := map[string]any{"id": int64(2), "value": 2.718281828459045, "name": "second", "active": false}
	for _, tt := range []struct {
		name     string
		input    any
		expected []map[string]any
	}{
		{"single", first, []map[string]any{first}},
		{"batch", []map[string]any{first, second}, []map[string]any{first, second}},
		{"leading empty record", []map[string]any{{}, first}, []map[string]any{first}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &ParquetWriter{}
			content, err := p.Encode(ctx, tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expected, readParquetRows(t, content))
		})
	}
}

func TestWriteBatch(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "parquet")
	first := map[string]any{"id": int64(1)}
	second := map[string]any{"id": int64(2)}
	third := map[string]any{"id": int64(3)}
	for _, batchFirst := range []bool{false, true} {
		name := "single record first"
		if batchFirst {
			name = "record list first"
		}
		t.Run(name, func(t *testing.T) {
			p := &ParquetWriter{}
			require.NoError(t, p.New(ctx))
			if batchFirst {
				require.NoError(t, p.Write(ctx, []map[string]any{first, second}))
			} else {
				require.NoError(t, p.Write(ctx, first))
				require.NoError(t, p.Write(ctx, second))
			}
			require.NoError(t, p.Write(ctx, third))
			content, err := p.Flush(ctx)
			require.NoError(t, err)
			require.Equal(t, []map[string]any{first, second, third}, readParquetRows(t, content))
		})
	}
}

func TestEmptyInput(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "parquet")
	for _, tt := range []struct {
		name  string
		input any
	}{
		{"nil map", map[string]any(nil)},
		{"empty map", map[string]any{}},
		{"nil list", []map[string]any(nil)},
		{"empty list", []map[string]any{}},
		{"empty records", []map[string]any{{}, nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &ParquetWriter{}
			content, err := p.Encode(ctx, tt.input)
			require.NoError(t, err)
			require.Empty(t, content)
			require.NoError(t, p.New(ctx))
			require.NoError(t, p.Write(ctx, tt.input))
			content, err = p.Flush(ctx)
			require.NoError(t, err)
			require.Empty(t, content)
		})
	}
	p := &ParquetWriter{}
	require.NoError(t, p.New(ctx))
	content, err := p.Flush(ctx)
	require.NoError(t, err)
	require.Empty(t, content)
}

func TestFlushPayloadOwnership(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "parquet")
	p := &ParquetWriter{}
	first := map[string]any{"id": int64(1), "name": "first"}
	second := map[string]any{"id": int64(2), "name": "a different second payload"}
	require.NoError(t, p.New(ctx))
	require.NoError(t, p.Write(ctx, first))
	content, err := p.Flush(ctx)
	require.NoError(t, err)
	saved := bytes.Clone(content)
	require.NoError(t, p.New(ctx))
	require.NoError(t, p.Write(ctx, second))
	next, err := p.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, saved, content)
	require.Equal(t, []map[string]any{first}, readParquetRows(t, content))
	require.Equal(t, []map[string]any{second}, readParquetRows(t, next))
}

func TestUnsupportedInput(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "parquet")
	for _, tt := range []struct {
		name  string
		input any
	}{
		{"scalar", "record"},
		{"unsupported value", map[string]any{"value": make(chan int)}},
		{"unknown null type", map[string]any{"value": nil}},
		{"unsupported nested value", map[string]any{"nested": map[string]any{"value": make(chan int)}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &ParquetWriter{}
			_, err := p.Encode(ctx, tt.input)
			require.Error(t, err)
			require.NoError(t, p.New(ctx))
			require.Error(t, p.Write(ctx, tt.input))
		})
	}
	p := &ParquetWriter{}
	_, err := p.Decode(ctx, nil)
	require.ErrorContains(t, err, "only supports encoding")
}
