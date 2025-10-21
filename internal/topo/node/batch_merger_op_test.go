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

package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestNewBatchMergerOp(t *testing.T) {
	op, err := NewBatchMergerOp("op1", &def.RuleOption{BufferLength: 10, SendError: true})
	require.NoError(t, err)
	assert.NotNil(t, op)
	assert.Equal(t, "op1", op.name)
}

func TestBatchMergerOpRun(t *testing.T) {
	testcases := []struct {
		name   string
		input  []any
		err    string
		expect int
	}{
		{
			name: "single tuple",
			input: []any{
				&xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{
						"b": 12,
					},
				},
			},
			expect: 1,
		},
		{
			name: "multiple tuples",
			input: []any{
				&xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{
						"b": 12,
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{
						"a": "a",
						"b": 20,
						"c": "hello",
					},
				},
			},
			expect: 2,
		},
		{
			name: "slice tuple",
			input: []any{
				&xsql.SliceTuple{
					SourceContent: model.SliceVal{nil, 12},
				},
				&xsql.SliceTuple{
					SourceContent: model.SliceVal{"a", 20},
				},
			},
			expect: 2,
		},
		{
			name: "transformed tuple list",
			input: []any{
				&xsql.TransformedTupleList{
					Maps: []map[string]any{
						{
							"a": "a",
							"b": 20,
							"c": "hello",
						},
						{
							"a": "a2",
						},
					},
				},
			},
			expect: 2,
		},
	}
	mc := mockclock.GetMockClock()
	ctx := mockContext.NewMockContext("testBatchMergerOpRun", "op1")
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			op, err := NewBatchMergerOp("test", &def.RuleOption{BufferLength: 10, SendError: true})
			require.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			require.NoError(t, err)
			errCh := make(chan error)
			op.Exec(ctx, errCh)
			for _, item := range tc.input {
				op.input <- item
				mc.Add(30 * time.Millisecond)
			}
			op.input <- xsql.BatchEOFTuple(time.Now())
			result := <-out
			if tc.err != "" {
				e, ok := result.(error)
				if ok {
					assert.EqualError(t, e, tc.err)
				} else {
					assert.Fail(t, "expected error", tc.err)
				}
			} else {
				wt, ok := result.(*xsql.WindowTuples)
				assert.True(t, ok, "expected WindowTuples, got %T", result)
				if tc.expect > 0 {
					assert.NotNil(t, wt)
					assert.Equal(t, tc.expect, len(wt.Content))
				} else {
					assert.Nil(t, result)
				}
			}
		})
	}
}

func TestBatchMergerOpErrorHandling(t *testing.T) {
	ctx := mockContext.NewMockContext("testBatchMergerOpErrorHandling", "op1")
	op, err := NewBatchMergerOp("test", &def.RuleOption{BufferLength: 10, SendError: true})
	require.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	require.NoError(t, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	testErr := assert.AnError
	op.input <- testErr
	result := <-out
	e, ok := result.(error)
	assert.True(t, ok)
	assert.Equal(t, testErr, e)
}

func TestBatchMergerOpWatermarkAndEOF(t *testing.T) {
	ctx := mockContext.NewMockContext("testBatchMergerOpWatermarkAndEOF", "op1")
	op, err := NewBatchMergerOp("test", &def.RuleOption{BufferLength: 10, SendError: true})
	require.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	require.NoError(t, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	watermark := &xsql.WatermarkTuple{Timestamp: time.Now()}
	op.input <- watermark
	result := <-out
	assert.Equal(t, watermark, result)
	eof := xsql.EOFTuple("")
	op.input <- eof
	result = <-out
	assert.Equal(t, eof, result)
}
