// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestOutputs(t *testing.T) {
	n := newDefaultNode("test", &def.RuleOption{})
	err := n.AddOutput(make(chan any), "rule.1_test")
	assert.NoError(t, err)
	err = n.AddOutput(make(chan any), "rule.2_test")
	assert.NoError(t, err)
	err = n.RemoveOutput("rule.1")
	assert.NoError(t, err)
	err = n.RemoveOutput("rule.4")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(n.outputs))
	assert.Len(t, n.outputSlice, 1)
	assert.Equal(t, "rule.2_test", n.outputSlice[0].name)
}

func TestCommonIngestCallsHookBeforeEOF(t *testing.T) {
	ctx := mockContext.NewMockContext("finalize", "op1")
	n := newDefaultSinkNode("test", &def.RuleOption{})
	n.ctx = ctx
	called := false
	_, processed := n.commonIngestWithControl(ctx, xsql.EOFTuple("done"), func(marker any) bool {
		require.Equal(t, xsql.EOFTuple("done"), marker)
		called = true
		return false
	})
	require.True(t, processed)
	require.True(t, called)
}

func TestMultipleOutputsBroadcast(t *testing.T) {
	ctx := mockContext.NewMockContext("multi", "op1")
	n := newDefaultNode("test", &def.RuleOption{})
	n.ctx = ctx
	output1 := make(chan any, 10)
	output2 := make(chan any, 10)
	err := n.AddOutput(output1, "rule.1_test")
	require.NoError(t, err)
	err = n.AddOutput(output2, "rule.2_test")
	require.NoError(t, err)
	tc := []struct {
		name string
		data any
	}{
		{
			name: "row broadcast",
			data: &xsql.Tuple{
				Ctx:       nil,
				Emitter:   "test",
				Message:   map[string]any{"a": 20},
				Timestamp: time.UnixMilli(123456789),
			},
		},
		{
			name: "collection broadcast",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Ctx:       nil,
						Emitter:   "test",
						Message:   map[string]any{"a": 30},
						Timestamp: time.UnixMilli(123456789),
					},
				},
			},
		},
		{
			name: "buffer data",
			data: &checkpoint.BufferOrEvent{
				Data: &xsql.Tuple{
					Emitter:   "test2",
					Message:   map[string]any{"a": 40},
					Timestamp: time.UnixMilli(123456789),
				},
				Channel: "test2",
			},
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			var result1, result2 any
			wg := &sync.WaitGroup{}
			wg.Add(3)
			go func() {
				defer wg.Done()
				n.Broadcast(tt.data)
			}()
			go func() {
				defer wg.Done()
				result1 = <-output1
			}()
			go func() {
				defer wg.Done()
				result2 = <-output2
			}()
			wg.Wait()
			assert.False(t, result1 == result2)
			assert.Equal(t, tt.data, result1)
			assert.Equal(t, tt.data, result2)
		})
	}
}

func TestBlockingOutputCanBeRemoved(t *testing.T) {
	ctx := mockContext.NewMockContext("remove-blocked", "op1")
	n := newDefaultNode("test", &def.RuleOption{DisableBufferFullDiscard: true})
	n.ctx = ctx
	output := make(chan any, 1)
	output <- "old"
	require.NoError(t, n.AddOutput(output, "rule.1_test"))

	broadcastDone := make(chan struct{})
	go func() {
		n.Broadcast("new")
		close(broadcastDone)
	}()

	select {
	case <-broadcastDone:
		t.Fatal("broadcast unexpectedly completed while the lossless output was full")
	case <-time.After(50 * time.Millisecond):
	}

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- n.RemoveOutput("rule.1")
	}()

	select {
	case err := <-removeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("removing a blocked output timed out")
	}
	select {
	case <-broadcastDone:
	case <-time.After(time.Second):
		t.Fatal("broadcast did not stop waiting after its output was removed")
	}
	require.Equal(t, "old", <-output)
}

func TestReplacingOutputCancelsOldEntry(t *testing.T) {
	n := newDefaultNode("test", &def.RuleOption{})
	require.NoError(t, n.AddOutput(make(chan any, 1), "same"))
	oldDone := n.outputs["same"].done

	require.NoError(t, n.AddOutput(make(chan any, 1), "same"))
	select {
	case <-oldDone:
	default:
		t.Fatal("replacing an output did not cancel the old entry")
	}
}

func TestDroppingUnbufferedOutputCanBeRemoved(t *testing.T) {
	ctx := mockContext.NewMockContext("remove-unbuffered", "op1")
	n := newDefaultNode("test", &def.RuleOption{})
	n.ctx = ctx
	require.NoError(t, n.AddOutput(make(chan any), "rule.1_test"))

	broadcastDone := make(chan struct{})
	go func() {
		n.Broadcast("new")
		close(broadcastDone)
	}()
	select {
	case <-broadcastDone:
		t.Fatal("broadcast unexpectedly completed with no receiver")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, n.RemoveOutput("rule.1"))
	select {
	case <-broadcastDone:
	case <-time.After(time.Second):
		t.Fatal("broadcast did not stop waiting after its output was removed")
	}
}

func BenchmarkBroadcastOutputs(b *testing.B) {
	for _, outputCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("outputs_%d", outputCount), func(b *testing.B) {
			ctx := mockContext.NewMockContext("benchmark", "broadcast")
			n := newDefaultNode("test", &def.RuleOption{})
			n.ctx = ctx
			outputs := make([]chan any, outputCount)
			for i := range outputs {
				outputs[i] = make(chan any, 1)
				require.NoError(b, n.AddOutput(outputs[i], fmt.Sprintf("output_%d", i)))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n.Broadcast(i)
				for _, output := range outputs {
					<-output
				}
			}
		})
	}
}

func TestSetQosPreservesBufferFullDiscardOption(t *testing.T) {
	tests := []struct {
		disableBufferFullDiscard         bool
		qos                              def.Qos
		expectedDisableBufferFullDiscard bool
	}{
		{disableBufferFullDiscard: false, qos: def.AtLeastOnce, expectedDisableBufferFullDiscard: false},
		{disableBufferFullDiscard: true, qos: def.AtLeastOnce, expectedDisableBufferFullDiscard: true},
	}
	for _, tt := range tests {
		n := newDefaultNode("test", &def.RuleOption{DisableBufferFullDiscard: tt.disableBufferFullDiscard})
		n.SetQos(tt.qos)
		assert.Equal(t, tt.expectedDisableBufferFullDiscard, n.disableBufferFullDiscard)
	}
}
