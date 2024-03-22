// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestJSON(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
	}{
		{"single", 1},
		{"multi", 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewDecodeOp("test", "streamName", "test1", &api.RuleOption{BufferLength: 10, SendError: true, Concurrency: tt.concurrency}, &ast.Options{FORMAT: "json"}, false, true, nil)
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx := mockContext.NewMockContext("test1", "decode_test")
			errCh := make(chan error)
			op.Exec(ctx, errCh)

			cases := []any{
				&xsql.Tuple{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.Tuple{Emitter: "test", Raw: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				errors.New("go through error"),
				&xsql.Tuple{Emitter: "test", Raw: []byte("\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.Tuple{Emitter: "test", Raw: []byte("[\"hello\"]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.Tuple{Emitter: "test", Raw: []byte("\"hello\""), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				"invalid",
			}
			expects := [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 1.0, "b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 1.0, "b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 3.0, "b": 4.0, "c": "hello"}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
				{errors.New("go through error")},
				{errors.New("invalid character ':' after top-level value")},
				{errors.New("only map[string]any inside a list is supported but got: hello")},
				{errors.New("unsupported decode result: hello")},
				{errors.New("unsupported data received: invalid")},
			}

			for i, c := range cases {
				op.input <- c
				for _, e := range expects[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						require.Equal(t, e.(error).Error(), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}
		})
	}
}

// Concurrency 1 - BenchmarkThrougput-16                  1        1548680100 ns/op
// Concurrency 10 - BenchmarkThrougput-16           1000000000               0.1553 ns/op
// This is useful when a node is much slower
func BenchmarkThrougput(b *testing.B) {
	op, err := NewDecodeOp("test", "streamName", "test1", &api.RuleOption{BufferLength: 10, SendError: true, Concurrency: 10, Debug: true}, &ast.Options{FORMAT: "mock"}, false, true, nil)
	assert.NoError(b, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(b, err)
	ctx := mockContext.NewMockContext("test1", "decode_test")
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	go func() {
		for i := 0; i < 100; i++ {
			op.input <- &xsql.Tuple{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}
		}
	}()
	for i := 0; i < 100; i++ {
		_ = <-out
	}
}

func TestJSONWithSchema(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		schema      map[string]*ast.JsonStreamField
	}{
		{"single", 1, map[string]*ast.JsonStreamField{
			"b": {
				Type: "float",
			},
		}},
		{"multi", 10, map[string]*ast.JsonStreamField{
			"b": {
				Type: "float",
			},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewDecodeOp("test", "streamName", "test1", &api.RuleOption{BufferLength: 10, SendError: true, Concurrency: tt.concurrency}, &ast.Options{FORMAT: "json", SHARED: true}, false, false, map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			})
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx := mockContext.NewMockContext("test1", "decode_test")
			errCh := make(chan error)
			op.Exec(ctx, errCh)

			cases := []any{
				&xsql.Tuple{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.Tuple{Emitter: "test", Raw: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
			}
			expects := [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1)}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1)}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(3)}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
			}

			for i, c := range cases {
				op.input <- c
				for _, e := range expects[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						assert.EqualError(t, e.(error), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}

			nctx := mockContext.NewMockContext("test2", "decode_test")
			op.AttachSchema(nctx, "streamName", tt.schema, false)
			cases = []any{
				&xsql.Tuple{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.Tuple{Emitter: "test", Raw: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
			}
			expectsWithSchema := [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1), "b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1), "b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(3), "b": 4.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
			}

			for i, c := range cases {
				op.input <- c
				for _, e := range expectsWithSchema[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						assert.EqualError(t, e.(error), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}

			op.DetachSchema(ctx.GetRuleId())
			cases = []any{
				&xsql.Tuple{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.Tuple{Emitter: "test", Raw: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
			}
			expects = [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"b": 4.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
			}
			for i, c := range cases {
				op.input <- c
				for _, e := range expects[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						assert.EqualError(t, e.(error), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	_, err := NewDecodeOp("test", "streamName", "test1", &api.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "cann"}, false, true, nil)
	assert.Error(t, err)
	assert.Equal(t, "cannot get converter from format cann, schemaId : format type cann not supported", err.Error())
}
