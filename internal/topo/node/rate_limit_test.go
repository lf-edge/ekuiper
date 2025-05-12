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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestNewRateLimit(t *testing.T) {
	ctx := mockContext.NewMockContext("test1", "new_test")
	rl, err := NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1s"})
	assert.NoError(t, err)
	assert.Equal(t, 0, rl.mergeStrategy)
	_, err = NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1ns"})
	assert.Error(t, err)
	assert.EqualError(t, err, "interval should be larger than 1ms")
	_, err = NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1s", "mergeField": "id"})
	assert.Error(t, err)
	assert.EqualError(t, err, "rate limit merge must define format")
	_, err = NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1s", "mergeField": "id", "format": "none"})
	assert.Error(t, err)
	assert.EqualError(t, err, "format type none not supported")
	_, err = NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1s", "mergeField": "id", "format": "delimited"})
	assert.Error(t, err)
	assert.EqualError(t, err, "format delimited does not support partial decode")
	_, err = NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1s", "merger": "none", "format": "delimited"})
	assert.Error(t, err)
	assert.EqualError(t, err, "merger none not found")
	modules.RegisterMerger("none", func(ctx api.StreamContext, schemaId string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		return nil, errors.New("mock error")
	})
	_, err = NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{"interval": "1s", "merger": "none", "format": "delimited"})
	assert.Error(t, err)
	assert.EqualError(t, err, "fail to initiate merge none: mock error")
}

func TestRateLimit(t *testing.T) {
	testcases := []struct {
		name        string
		sendCount   int
		interval    time.Duration
		expectItems []any
	}{
		{ // sending gap is 300ms
			name:      "normal",
			sendCount: 10,
			interval:  time.Second,
			expectItems: []any{
				&xsql.WatermarkTuple{},
				&xsql.WatermarkTuple{},
				&xsql.RawTuple{
					Rawdata: []byte{3},
				},
				&xsql.WatermarkTuple{},
				&xsql.RawTuple{
					Rawdata: []byte{6},
				},
				&xsql.WatermarkTuple{},
				&xsql.RawTuple{
					Rawdata: []byte{9},
				},
			},
		},
	}
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			ctx, cancel := mockContext.NewMockContext("test1", "batch_test").WithCancel()
			op, err := NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{
				"interval": "1s",
			})
			assert.NoError(t, err)
			out := make(chan any, 10)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)

			timex.Set(100)
			errCh := make(chan error)
			op.Exec(ctx, errCh)
			for i := 0; i < tc.sendCount; i++ {
				op.input <- &xsql.RawTuple{
					Rawdata: []byte{uint8(i)},
				}
				fmt.Printf("send input at %d\n", timex.GetNowInMilli())
				if i%3 == 0 {
					op.input <- &xsql.WatermarkTuple{}
					fmt.Printf("send watermark at %d\n", timex.GetNowInMilli())
				}
				time.Sleep(10 * time.Millisecond)
				timex.Add(300 * time.Millisecond)
			}
			timex.Add(2 * time.Second)
			cancel()
			// make sure op has done all sending
			for {
				processed := op.statManager.GetMetrics()[2]
				if processed == int64(tc.sendCount) {
					close(out)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			r := make([]any, 0, len(tc.expectItems))
			for ele := range out {
				r = append(r, ele)
			}
			assert.Equal(t, tc.expectItems, r)
		})
	}
}

func TestRateLimitMerge(t *testing.T) {
	timex.Set(3333)
	testcases := []struct {
		name        string
		sendCount   int
		interval    time.Duration
		expectItems []any
	}{
		{ // sending gap is 300ms
			name:      "normal",
			sendCount: 10,
			interval:  time.Second,
			expectItems: []any{
				&xsql.Tuple{
					Message: map[string]any{
						"frames": []any{
							map[string]any{
								"data": []byte(`{"id":0, "value":2}`),
							},
							map[string]any{
								"data": []byte(`{"id":1, "value":3}`),
							},
						},
					},
					Timestamp: time.UnixMilli(4333),
				},
				&xsql.Tuple{
					Message: map[string]any{
						"frames": []any{
							map[string]any{
								"data": []byte(`{"id":0, "value":6}`),
							},
							map[string]any{
								"data": []byte(`{"id":1, "value":5}`),
							},
						},
					},
					Timestamp: time.UnixMilli(5333),
				},
				&xsql.Tuple{
					Message: map[string]any{
						"frames": []any{
							map[string]any{
								"data": []byte(`{"id":0, "value":8}`),
							},
							map[string]any{
								"data": []byte(`{"id":1, "value":9}`),
							},
						},
					},
					Timestamp: time.UnixMilli(6333),
				},
			},
		},
	}
	modules.RegisterConverter("mockp", func(ctx api.StreamContext, _ string, logicalSchema map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &message.MockPartialConverter{}, nil
	})
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := mockContext.NewMockContext("test1", "batch_test").WithCancel()
			op, err := NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{
				"interval":   "1s",
				"mergeField": "id",
				"format":     "mockp",
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, op.mergeStrategy)
			out := make(chan any, 10)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)

			errCh := make(chan error)
			op.Exec(ctx, errCh)
			for i := 0; i < tc.sendCount; i++ {
				op.input <- &xsql.RawTuple{
					Rawdata: []byte(fmt.Sprintf(`{"id":%d, "value":%d}`, i%2, i)),
				}
				timex.Add(300 * time.Millisecond)
			}
			cancel()
			// make sure op has done all sending
			for {
				processed := op.statManager.GetMetrics()[2]
				if processed == int64(tc.sendCount) {
					close(out)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			r := make([]any, 0, len(tc.expectItems))
			for ele := range out {
				r = append(r, ele)
			}
			assert.Equal(t, tc.expectItems, r)
		})
	}
}

func TestRateLimitCustomMerge(t *testing.T) {
	testcases := []struct {
		name        string
		sendCount   int
		interval    time.Duration
		expectItems []any
	}{
		{ // sending gap is 300ms
			name:      "normal",
			sendCount: 10,
			interval:  time.Second,
			expectItems: []any{
				errors.New("rate limit merge only supports raw but got"),
				&xsql.Tuple{
					Message: map[string]any{
						"frames": []any{
							map[string]any{
								"data": []byte(`{"id":0, "value":2}`),
							},
							map[string]any{
								"data": []byte(`{"id":1, "value":3}`),
							},
						},
					},
				},
				&xsql.Tuple{
					Message: map[string]any{
						"frames": []any{
							map[string]any{
								"data": []byte(`{"id":0, "value":6}`),
							},
							map[string]any{
								"data": []byte(`{"id":1, "value":5}`),
							},
						},
					},
				},
				&xsql.Tuple{
					Message: map[string]any{
						"frames": []any{
							map[string]any{
								"data": []byte(`{"id":0, "value":8}`),
							},
							map[string]any{
								"data": []byte(`{"id":1, "value":9}`),
							},
						},
					},
				},
			},
		},
	}
	modules.RegisterMerger("mock", func(ctx api.StreamContext, schemaId string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		return &message.MockMerger{}, nil
	})
	mc := mockclock.GetMockClock()
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := mockContext.NewMockContext("test1", "batch_test").WithCancel()
			op, err := NewRateLimitOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, nil, map[string]any{
				"interval":      "1s",
				"merger":        "mock",
				"format":        "json",
				"payloadFormat": "json",
			})
			assert.NoError(t, err)
			assert.Equal(t, 2, op.mergeStrategy)
			out := make(chan any, 10)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)

			errCh := make(chan error)
			op.Exec(ctx, errCh)
			op.input <- &xsql.Tuple{
				Message: map[string]any{"test": 1},
			}
			op.ResetSchema(ctx, map[string]*ast.JsonStreamField{})
			for i := 0; i < tc.sendCount; i++ {
				op.input <- &xsql.RawTuple{
					Rawdata: []byte(fmt.Sprintf(`{"id":%d, "value":%d}`, i%2, i)),
				}
				mc.Add(300 * time.Millisecond)
			}
			cancel()
			// make sure op has done all sending
			for {
				processed := op.statManager.GetMetrics()[2]
				if processed == int64(tc.sendCount)+1 {
					close(out)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			r := make([]any, 0, len(tc.expectItems))
			for ele := range out {
				r = append(r, ele)
			}
			for i := 0; i < len(tc.expectItems); i++ {
				switch tc.expectItems[i].(type) {
				case *xsql.Tuple:
					expTuples := tc.expectItems[i].(*xsql.Tuple)
					gotTuples := r[i].(*xsql.Tuple)
					require.Equal(t, expTuples.Message, gotTuples.Message)
				case error:
					expErr := tc.expectItems[i].(error)
					gotErr := r[i].(error)
					require.True(t, strings.Contains(gotErr.Error(), expErr.Error()))
				}
			}
		})
	}
}
