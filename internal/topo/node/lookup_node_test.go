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
	"fmt"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/lookup"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestLookupInit(t *testing.T) {
	ctx := mockContext.NewMockContext("test1", "testInit")
	t.Run("error decode lookup conf", func(t *testing.T) {
		_, err := NewLookupNode(ctx, "test2", false, []string{"la", "lb"}, []string{"test1"}, ast.LEFT_JOIN, []ast.Expr{}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{"lookup": map[string]any{"cacheTtl": "nosense"}})
		assert.Error(t, err)
		assert.EqualError(t, err, "1 error(s) decoding:\n\n* error decoding 'cacheTtl': time: invalid duration \"nosense\"")
	})

	t.Run("wrong decoder format", func(t *testing.T) {
		_, err := NewLookupNode(ctx, "test2", true, []string{"la", "lb"}, []string{"test1"}, ast.LEFT_JOIN, []ast.Expr{}, &ast.Options{TYPE: "mock", FORMAT: "json1"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{})
		assert.Error(t, err)
		assert.EqualError(t, err, "cannot get converter from format json1, schemaId : format type json1 not supported")
	})

	t.Run("wrong payload field", func(t *testing.T) {
		_, err := NewLookupNode(ctx, "test2", true, []string{"la", "lb"}, []string{"test1"}, ast.LEFT_JOIN, []ast.Expr{}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{"payloadField": 1})
		assert.Error(t, err)
		assert.EqualError(t, err, "1 error(s) decoding:\n\n* 'payloadField' expected type 'string', got unconvertible type 'int', value: '1'")
	})

	t.Run("missing payload format", func(t *testing.T) {
		_, err := NewLookupNode(ctx, "test2", true, []string{"la", "lb"}, []string{"test1"}, ast.LEFT_JOIN, []ast.Expr{}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{"payloadField": "a"})
		assert.Error(t, err)
		assert.EqualError(t, err, "payloadFormat and payloadField must set together")
	})

	t.Run("wrong payload format", func(t *testing.T) {
		_, err := NewLookupNode(ctx, "test2", true, []string{"la", "lb"}, []string{"test1"}, ast.LEFT_JOIN, []ast.Expr{}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{"payloadField": "a", "payloadFormat": "json1"})
		assert.Error(t, err)
		assert.EqualError(t, err, "cannot get payload converter from payloadFormat json1, schemaId : format type json1 not supported")
	})
}

func TestLookup(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		result any // join tuple or error
	}{
		{
			name: "normal",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": 2},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 2.0, "lb": 2.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "lookup error",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "wrong"},
			},
			result: errors.New("mock lookup error"),
		},
		{
			name: "window",
			input: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "stream1",
						Message: map[string]any{"a": 2},
					},
					&xsql.Tuple{
						Emitter: "stream1",
						Message: map[string]any{"a": 3},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 2.0, "lb": 2.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 3},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 3},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 2.0, "lb": 2.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 3},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "empty",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "empty"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "empty"},
							},
						},
					},
				},
			},
		},
		{
			name: "decode error",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "jsonerror"},
			},
			// decode error, but still have result
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "jsonerror"},
							},
						},
					},
				},
			},
		},
		{
			name: "decode to array",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "array"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "array"},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "array"},
							},
							&xsql.Tuple{
								Emitter:   "test2",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
	}
	modules.RegisterLookupSource("mock", func() api.Source {
		return &MockLookupBytes{}
	})

	ctx, cancel := mockContext.NewMockContext("testRule", "test").WithCancel()
	defer cancel()
	op, err := NewLookupNode(ctx, "test2", true, []string{"la", "lb"}, []string{"test1"}, ast.LEFT_JOIN, []ast.Expr{&ast.FieldRef{
		StreamName: "",
		Name:       "a",
	}}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{})
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error, 1)
	op.Exec(ctx, errCh)
	err = <-errCh
	assert.Error(t, err)
	assert.EqualError(t, err, "lookup table test2 is not found")
	// run table
	err = lookup.CreateInstance("test2", "mock", &ast.Options{
		DATASOURCE: "test2",
		TYPE:       "mock",
		KIND:       "lookup",
		KEY:        "id",
	})
	assert.NoError(t, err)
	op.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.input
			r := <-out
			assert.Equal(t, tt.result, r)
		})
	}
}

func TestLookupInner(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		result any // join tuple or error
	}{
		{
			name: "normal",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": 2},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 2.0, "lb": 2.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "empty",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "empty"},
			},
			result: nil,
		},
		{
			name: "lookup error",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "wrong"},
			},
			result: errors.New("mock lookup error"),
		},
		{
			name: "decode error",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "jsonerror"},
			},
			// decode error, inner join has no result
			result: nil,
		},
		{
			name: "window",
			input: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "stream1",
						Message: map[string]any{"a": 2},
					},
					&xsql.Tuple{
						Emitter: "stream1",
						Message: map[string]any{"a": 3},
					},
				},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 2.0, "lb": 2.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 2},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 3},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 3},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 2.0, "lb": 2.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": 3},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "window empty",
			input: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Emitter: "stream1",
						Message: map[string]any{"a": "empty"},
					},
				},
			},
			result: nil,
		},
		{
			name: "decode to array",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "array"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "array"},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 1.0, "lb": 1.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "array"},
							},
							&xsql.Tuple{
								Emitter:   "testInner",
								Message:   map[string]any{"la": 3.0, "lb": 4.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
	}
	modules.RegisterLookupSource("mock", func() api.Source {
		return &MockLookupBytes{}
	})

	ctx, cancel := mockContext.NewMockContext("testLookupInner", "test").WithCancel()
	defer cancel()
	op, err := NewLookupNode(ctx, "testInner", true, []string{"la", "lb"}, []string{"test1"}, ast.INNER_JOIN, []ast.Expr{&ast.FieldRef{
		StreamName: "",
		Name:       "a",
	}}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{})
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error, 1)
	op.Exec(ctx, errCh)
	err = <-errCh
	assert.Error(t, err)
	assert.EqualError(t, err, "lookup table testInner is not found")
	// run table
	err = lookup.CreateInstance("testInner", "mock", &ast.Options{
		DATASOURCE: "testInner",
		TYPE:       "mock",
		KIND:       "lookup",
		KEY:        "id",
	})
	assert.NoError(t, err)
	op.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.input
			if tt.result != nil {
				r := <-out
				assert.Equal(t, tt.result, r)
			}
		})
	}
}

func TestLookupPayload(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		result any // join tuple or error
	}{
		{
			name: "normal payload",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payload"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "payload"},
							},
							&xsql.Tuple{
								Emitter:   "testP",
								Message:   map[string]any{"la": 11.0, "lb": 12.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name:   "invalid stream input",
			input:  "test",
			result: errors.New("run lookup node error: invalid input type but got string(test)"),
		},
		{
			name: "use cache",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payload"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "payload"},
							},
							&xsql.Tuple{
								Emitter:   "testP",
								Message:   map[string]any{"la": 11.0, "lb": 12.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "normal payload array",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payloadA"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "payloadA"},
							},
							&xsql.Tuple{
								Emitter:   "testP",
								Message:   map[string]any{"la": 11.0, "lb": 12.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "payloadEmpty",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payloadEmpty"},
			},
			result: nil,
		},
		{
			name: "payload not bytes",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payload not bytes"},
			},
			result: nil,
		},
		{
			name: "array payload array",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payloadAA"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "payloadAA"},
							},
							&xsql.Tuple{
								Emitter:   "testP",
								Message:   map[string]any{"la": 11.0, "lb": 12.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
		{
			name: "array payload array array",
			input: &xsql.Tuple{
				Emitter: "stream1",
				Message: map[string]any{"a": "payloadAAA"},
			},
			result: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.Row{
							&xsql.Tuple{
								Emitter: "stream1",
								Message: map[string]any{"a": "payloadAAA"},
							},
							&xsql.Tuple{
								Emitter:   "testP",
								Message:   map[string]any{"la": 11.0, "lb": 12.0},
								Timestamp: timex.GetNow(),
							},
						},
					},
				},
			},
		},
	}
	modules.RegisterLookupSource("mock", func() api.Source {
		return &MockLookupBytes{}
	})

	ctx, cancel := mockContext.NewMockContext("testRule", "test").WithCancel()
	defer cancel()
	op, err := NewLookupNode(ctx, "testP", true, []string{"la", "lb"}, []string{"test1"}, ast.INNER_JOIN, []ast.Expr{&ast.FieldRef{
		StreamName: "",
		Name:       "a",
	}}, &ast.Options{TYPE: "mock", FORMAT: "json"}, &def.RuleOption{BufferLength: 10, SendError: true}, map[string]any{
		"payloadField": "payload", "payloadFormat": "json", "lookup": map[string]any{"cache": true, "cacheTtl": "1s"},
	})
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error, 1)
	op.Exec(ctx, errCh)
	err = <-errCh
	assert.Error(t, err)
	assert.EqualError(t, err, "lookup table testP is not found")
	// run table
	err = lookup.CreateInstance("testP", "mock", &ast.Options{
		DATASOURCE: "testP",
		TYPE:       "mock",
		KIND:       "lookup",
		KEY:        "id",
	})
	assert.NoError(t, err)
	op.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.input
			if tt.result != nil {
				r := <-out
				assert.Equal(t, tt.result, r)
			}
		})
	}
	timex.Add(20 * time.Second)
}

type MockLookupBytes struct{}

func (m *MockLookupBytes) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *MockLookupBytes) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockLookupBytes) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *MockLookupBytes) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([][]byte, error) {
	switch values[0] {
	case "wrong":
		return nil, fmt.Errorf("mock lookup error")
	case "empty":
		return nil, nil
	case "jsonerror":
		return [][]byte{
			[]byte("{wrong jso}n"),
		}, nil
	case "array":
		return [][]byte{
			[]byte(`[{"la":1,"lb":1,"lc":1},{"la":3,"lb":4,"lc":1}]`),
		}, nil
	case "payload":
		return [][]byte{
			[]byte(`{"la":1,"lb":1,"payload":"{\"la\":11, \"lb\":12, \"lc\":1}"}`),
		}, nil
	case "payloadA":
		return [][]byte{
			[]byte(`{"la":1,"lb":1,"payload":"[{\"la\":11, \"lb\":12, \"lc\":1}]"}`),
		}, nil
	case "payloadAA":
		return [][]byte{
			[]byte(`[{"la":1,"lb":1,"payload":"{\"la\":11, \"lb\":12, \"lc\":1}"},{"la":1,"lb":1,"payload":"invalid"}]`),
		}, nil
	case "payloadAAA":
		return [][]byte{
			[]byte(`[{"la":1,"lb":1,"payload":"[{\"la\":11, \"lb\":12, \"lc\":1}]"}]`),
		}, nil
	case "payloadEmpty":
		return [][]byte{
			[]byte(`{"la":1,"lb":1,"data":"[{\"la\":11, \"lb\":12, \"lc\":1}]"}`),
		}, nil
	case "payloadNotByte":
		return [][]byte{
			[]byte(`{"la":1,"lb":1,"payload":34`),
		}, nil
	default:
		return [][]byte{
			[]byte(`{"la":1,"lb":1,"lc":1}`),
			[]byte(`{"la":2,"lb":2,"lc":1}`),
			[]byte(`{"la":3,"lb":4,"lc":1}`),
		}, nil
	}
}
