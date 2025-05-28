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
