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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestNewSinkNode(t *testing.T) {
	ctx := mockContext.NewMockContext("testSink", "sink")
	tests := []struct {
		name           string
		sc             *model.SinkConf
		isRetry        bool
		resendInterval time.Duration
		bufferLength   int
	}{
		{
			name: "normal sink",
			sc: &model.SinkConf{
				ResendInterval:       0,
				MemoryCacheThreshold: 1024,
			},
			isRetry:        false,
			resendInterval: 0,
			bufferLength:   1024,
		},
		{
			name: "linear cache sink",
			sc: &model.SinkConf{
				ResendInterval:       cast.DurationConf(100 * time.Millisecond),
				EnableCache:          true,
				MemoryCacheThreshold: 10,
			},
			isRetry:        false,
			resendInterval: 100 * time.Millisecond,
			bufferLength:   10,
		},
		{
			name: "retry cache normal sink",
			sc: &model.SinkConf{
				ResendInterval:       cast.DurationConf(100 * time.Millisecond),
				EnableCache:          true,
				MemoryCacheThreshold: 1024,
				ResendAlterQueue:     true,
			},
			isRetry: false,
			// resend interval is set but no use
			resendInterval: 100 * time.Millisecond,
			bufferLength:   1024,
		},
		{
			name: "retry cache resend sink",
			sc: &model.SinkConf{
				ResendInterval:       cast.DurationConf(100 * time.Millisecond),
				EnableCache:          true,
				MemoryCacheThreshold: 10,
				ResendAlterQueue:     true,
			},
			isRetry: true,
			// resend interval is set but no use
			resendInterval: 100 * time.Millisecond,
			bufferLength:   10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newSinkNode(ctx, "test", def.RuleOption{
				BufferLength: 1024,
			}, 1, &SinkConf{
				SinkConf:     *tt.sc,
				BufferLength: 1024,
			}, tt.isRetry)
			assert.Equal(t, tt.resendInterval, n.resendInterval, "resend interval")
			assert.Equal(t, tt.bufferLength, cap(n.input))
		})
	}
}

func TestRetry(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("resendout", "sink").WithCancel()
	s := &mockResendSink{failTimes: 2}
	n, err := NewBytesSinkNode(ctx, "resendout_sink", s, def.RuleOption{
		BufferLength: 1024,
	}, 1, &SinkConf{
		SinkConf: model.SinkConf{
			ResendInterval:       cast.DurationConf(100 * time.Millisecond),
			EnableCache:          true,
			MemoryCacheThreshold: 10,
		},
	}, true)
	assert.NoError(t, err)
	data := &xsql.RawTuple{
		Emitter:   "",
		Timestamp: time.UnixMilli(1),
	}
	errCh := make(chan error, 1)
	n.Exec(ctx, errCh)
	n.input <- data
	for {
		timex.Add(50 * time.Millisecond)
		processed := n.statManager.GetMetrics()[2]
		if processed == int64(1) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	assert.Equal(t, s.val, data)
}

func TestResendOut(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("resendout", "sink").WithCancel()
	s := &mockResendSink{failTimes: 10}
	n, err := NewBytesSinkNode(ctx, "resendout_sink", s, def.RuleOption{
		BufferLength: 1024,
	}, 1, &SinkConf{
		SinkConf: model.SinkConf{
			ResendInterval:       cast.DurationConf(100 * time.Millisecond),
			EnableCache:          true,
			MemoryCacheThreshold: 10,
			ResendAlterQueue:     true,
		},
	}, true)
	assert.NoError(t, err)
	alertCh := make(chan any, 10)
	n.SetResendOutput(alertCh)
	data := &xsql.RawTuple{
		Emitter:   "",
		Timestamp: time.UnixMilli(1),
	}
	errCh := make(chan error, 1)
	n.Exec(ctx, errCh)
	go func() {
		n.input <- data
	}()
	got := false
	select {
	case d := <-alertCh:
		assert.Equal(t, data, d)
		got = true
	case e := <-errCh:
		assert.NoError(t, e)
	}
	cancel()
	assert.True(t, got)
}

type mockResendSink struct {
	failTimes int
	val       any
}

func (m *mockResendSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *mockResendSink) Close(ctx api.StreamContext) error {
	return nil
}

func (m *mockResendSink) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *mockResendSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	if m.failTimes > 0 {
		m.failTimes--
		return errorx.NewIOErr("fake error")
	}
	m.val = item
	return nil
}

var _ api.BytesCollector = &mockResendSink{}

// mockTupleCollector is a mock sink for testing tuple collection
type mockTupleCollector struct {
	collected     []api.MessageTuple
	collectedList []api.MessageTuple
}

func (m *mockTupleCollector) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *mockTupleCollector) Close(ctx api.StreamContext) error {
	return nil
}

func (m *mockTupleCollector) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *mockTupleCollector) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	m.collected = append(m.collected, item)
	return nil
}

func (m *mockTupleCollector) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	items.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
		m.collectedList = append(m.collectedList, tuple)
		return true
	})
	return nil
}

var _ api.TupleCollector = &mockTupleCollector{}

// mockConverter is a mock converter for testing
type mockConverter struct {
	result any
	err    error
}

func (m *mockConverter) Encode(ctx api.StreamContext, d any) ([]byte, error) {
	return nil, nil
}

func (m *mockConverter) Decode(ctx api.StreamContext, b []byte) (any, error) {
	return m.result, m.err
}

func TestDecodeAndCollect(t *testing.T) {
	ctx := mockContext.NewMockContext("testDecode", "sink")

	tests := []struct {
		name            string
		decodeResult    any
		decodeErr       error
		expectError     bool
		expectCollected int
		expectList      int
	}{
		{
			name:            "single map result",
			decodeResult:    map[string]any{"id": 1, "name": "test"},
			expectCollected: 1,
			expectList:      0,
		},
		{
			name:            "[]map result",
			decodeResult:    []map[string]any{{"id": 1}, {"id": 2}},
			expectCollected: 0,
			expectList:      2,
		},
		{
			name:            "[]any with maps result",
			decodeResult:    []any{map[string]any{"id": 1}, map[string]any{"id": 2}, map[string]any{"id": 3}},
			expectCollected: 0,
			expectList:      3,
		},
		{
			name:            "[]any with non-map result",
			decodeResult:    []any{"not a map"},
			expectError:     true,
			expectCollected: 0,
			expectList:      0,
		},
		{
			name:            "unsupported type result",
			decodeResult:    "string result",
			expectError:     true,
			expectCollected: 0,
			expectList:      0,
		},
		{
			name:            "decode error",
			decodeErr:       assert.AnError,
			expectError:     true,
			expectCollected: 0,
			expectList:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &mockTupleCollector{}
			conv := &mockConverter{result: tt.decodeResult, err: tt.decodeErr}
			rawTuple := &xsql.RawTuple{
				Rawdata:   []byte(`{"test": "data"}`),
				Timestamp: time.Now(),
				Metadata:  map[string]any{"source": "test"},
			}

			err := decodeAndCollect(ctx, sink, rawTuple, conv)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectCollected, len(sink.collected), "collected count")
			assert.Equal(t, tt.expectList, len(sink.collectedList), "collectList count")
		})
	}
}

func TestCreateTupleCollect(t *testing.T) {
	ctx := mockContext.NewMockContext("testTupleCollect", "sink")
	conv := &mockConverter{result: map[string]any{"decoded": true}}

	collectFn := createTupleCollect(conv)
	assert.NotNil(t, collectFn)

	t.Run("handles RawTuple", func(t *testing.T) {
		sink := &mockTupleCollector{}
		rawTuple := &xsql.RawTuple{
			Rawdata:   []byte(`{}`),
			Timestamp: time.Now(),
		}

		err := collectFn(ctx, sink, rawTuple)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sink.collected))
	})

	t.Run("handles MessageTuple", func(t *testing.T) {
		sink := &mockTupleCollector{}
		tuple := &xsql.Tuple{
			Message: map[string]any{"key": "value"},
		}

		err := collectFn(ctx, sink, tuple)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sink.collected))
	})

	t.Run("handles MessageTupleList", func(t *testing.T) {
		sink := &mockTupleCollector{}
		tuples := &xsql.TransformedTupleList{
			Content: []api.MessageTuple{
				&xsql.Tuple{Message: map[string]any{"id": 1}},
				&xsql.Tuple{Message: map[string]any{"id": 2}},
			},
		}

		err := collectFn(ctx, sink, tuples)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(sink.collectedList))
	})

	t.Run("handles error type", func(t *testing.T) {
		sink := &mockTupleCollector{}
		err := collectFn(ctx, sink, assert.AnError)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sink.collected))
		// Check error message is in the tuple
		assert.NotNil(t, sink.collected[0].ToMap()["error"])
	})

	t.Run("returns error for unknown type", func(t *testing.T) {
		sink := &mockTupleCollector{}
		err := collectFn(ctx, sink, "unknown type")
		assert.Error(t, err)
	})
}
