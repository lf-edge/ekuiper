// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package node

import (
	"errors"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestEncodeJSON(t *testing.T) {
	tests := []struct {
		name string
		in   any
		out  any
	}{
		{
			name: "normal",
			in:   &xsql.Tuple{Message: map[string]any{"name": "joe", "age": 20}},
			out:  &xsql.RawTuple{Rawdata: []byte(`{"age":20,"name":"joe"}`)},
		},
		{
			name: "list",
			in: &xsql.TransformedTupleList{
				Maps: []map[string]any{
					{"name": "joe", "age": 20},
					{"name": "tom", "age": 21},
				},
				Content: []api.MessageTuple{
					&xsql.Tuple{Message: map[string]any{"name": "joe", "age": 20}},
					&xsql.Tuple{Message: map[string]any{"name": "tom", "age": 21}},
				},
			},
			out: &xsql.RawTuple{Rawdata: []byte(`[{"age":20,"name":"joe"},{"age":21,"name":"tom"}]`), Timestamp: timex.GetNow()},
		},
		{
			name: "unknown type",
			in:   12,
			out:  errors.New("receive unsupported data 12"),
		},
		{
			name: "bytes",
			in:   &xsql.RawTuple{Rawdata: []byte("test")},
			out:  &xsql.RawTuple{Rawdata: []byte("test")},
		},
		{
			name: "prop and meta copy",
			in:   &xsql.Tuple{Message: map[string]any{"name": "joe", "age": 20}, Metadata: map[string]any{"topic": "demo"}, Props: map[string]string{"{{.a}}": "1"}},
			out:  &xsql.RawTuple{Rawdata: []byte(`{"age":20,"name":"joe"}`), Metadata: map[string]any{"topic": "demo"}, Props: map[string]string{"{{.a}}": "1"}},
		},
		{
			name: "list prop copy",
			in: &xsql.TransformedTupleList{
				Maps: []map[string]any{
					{"name": "joe", "age": 20},
					{"name": "tom", "age": 21},
				},
				Content: []api.MessageTuple{
					&xsql.Tuple{Message: map[string]any{"name": "joe", "age": 20}, Timestamp: timex.GetNow()},
					&xsql.Tuple{Message: map[string]any{"name": "tom", "age": 21}, Timestamp: timex.GetNow()},
				},
				Props: map[string]string{"{{.a}}": "1"},
			},
			out: &xsql.RawTuple{Rawdata: []byte(`[{"age":20,"name":"joe"},{"age":21,"name":"tom"}]`), Props: map[string]string{"{{.a}}": "1"}, Timestamp: timex.GetNow()},
		},
	}
	ctx := mockContext.NewMockContext("test1", "encode_test")
	op, err := NewEncodeOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, &SinkConf{Format: "json"})
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.in
			r := <-out
			assert.Equal(t, tt.out, r)
		})
	}
}

func TestEncodeSlice2JSON(t *testing.T) {
	tests := []struct {
		name string
		in   any
		out  string
	}{
		{
			name: "normal",
			in:   &xsql.SliceTuple{SourceContent: model.SliceVal{"joe", 20}},
			out:  `{"age":20,"name":"joe"}`,
		},
		{
			name: "list",
			in:   []*xsql.SliceTuple{{SourceContent: model.SliceVal{"joe", 20}}, {SourceContent: model.SliceVal{"john", 40}}},
			out:  "[{\"age\":20,\"name\":\"joe\"},{\"age\":40,\"name\":\"john\"}]",
		},
	}
	ctx := mockContext.NewMockContext("test1", "encode_test")
	op, err := NewEncodeOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, map[string]*ast.JsonStreamField{
		"age":  {HasIndex: true, Index: 1},
		"name": {HasIndex: true, Index: 0},
	}, &SinkConf{Format: "json"})
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.in
			r := <-out
			rt, ok := r.(*xsql.RawTuple)
			require.True(t, ok)
			assert.Equal(t, tt.out, string(rt.Rawdata))
		})
	}
}

func TestEncodeValidate(t *testing.T) {
	ctx := mockContext.NewMockContext("test1", "encode_test")
	_, err := NewEncodeOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, &SinkConf{Format: "cann"})
	assert.Error(t, err)
	assert.Equal(t, "format type cann not supported", err.Error())
}
