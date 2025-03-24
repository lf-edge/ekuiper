// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestNewErr(t *testing.T) {
	ctx := mockContext.NewMockContext("testNewErr", "op1")
	_, err := NewBatchWriterOp(ctx, "op1", nil, nil, &SinkConf{Format: "nop"})
	require.EqualError(t, err, "format type nop not supported")
}

func TestBatchWriterRun(t *testing.T) {
	testcases := []struct {
		name   string
		input  []any
		err    string
		expect string
	}{
		{
			name:  "error type",
			input: []any{45}, // invalid input type
			err:   "unknown data type: int",
		},
		{
			name: "multiple single",
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
			expect: "b\n12\n20",
		},
		{
			name: "multiple batch",
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
				&xsql.TransformedTupleList{
					Maps: []map[string]any{
						{
							"b": 12,
						},
						{
							"a": "a3",
							"b": 13,
						},
					},
				},
			},
			expect: "a,b,c\na,20,hello\na2,,\n12\n13",
		},
	}
	mc := mockclock.GetMockClock()
	ctx := mockContext.NewMockContext("testNewErr", "op1")
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// TODO sink schema does not work yet
			op, err := NewBatchWriterOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, map[string]*ast.JsonStreamField{
				"a": nil,
				"b": nil,
			}, &SinkConf{
				SendSingle: true,
				Format:     "delimited",
				HasHeader:  true,
			})
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
			// wait for output
			result := <-out
			if tc.err != "" {
				e, ok := result.(error)
				if ok {
					assert.EqualError(t, e, tc.err)
				} else {
					assert.Fail(t, "expected error", tc.err)
				}
			} else {
				e, ok := result.(*xsql.RawTuple)
				assert.True(t, ok)
				assert.Equal(t, tc.expect, string(e.Raw()))
			}
		})
	}
}
