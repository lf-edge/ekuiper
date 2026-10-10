// Copyright 2026 EMQ Technologies Co., Ltd.
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

package node_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

// dualInputResponder records checkpoint triggers fired through a real
// operator loop. It performs no broadcast or snapshot.
type dualInputResponder struct {
	mu    sync.Mutex
	fired []int64
}

func (r *dualInputResponder) TriggerCheckpoint(id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fired = append(r.fired, id)
	return nil
}

func (r *dualInputResponder) GetName() string {
	return "op"
}

func (r *dualInputResponder) triggers() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.fired...)
}

// receiveUnwrappedOutput skips barrier control messages and unwraps backlog
// rows and window emissions broadcast as BufferOrEvent under QoS >= 1.
func receiveUnwrappedOutput(t *testing.T, output <-chan any) any {
	t.Helper()
	for {
		select {
		case got := <-output:
			if boe, ok := got.(*checkpoint.BufferOrEvent); ok {
				if _, isBarrier := boe.Data.(*checkpoint.Barrier); isBarrier {
					continue
				}
				return boe.Data
			}
			return got
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for operator output")
			return nil
		}
	}
}

func requireNoOutput(t *testing.T, output <-chan any) {
	t.Helper()
	select {
	case got := <-output:
		t.Fatalf("unexpected operator output: %#v", got)
	default:
	}
}

// TestWatermarkOpDualInputBacklogDrain proves the watermark consumption
// loop drains the aligner backlog. Graph topologies allow watermarks with
// multiple inputs, so this loop must consult NextDue like any other
// multi-input operator.
func TestWatermarkOpDualInputBacklogDrain(t *testing.T) {
	base := time.UnixMilli(1000)
	options := &def.RuleOption{BufferLength: 16}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	op := node.NewWatermarkOp("watermark", false, []string{"demo1", "demo2"}, options)
	op.SetQos(def.ExactlyOnce)
	op.AddInputCount()
	op.AddInputCount()
	responder := &dualInputResponder{}
	op.SetBarrierHandler(checkpoint.NewBarrierAligner(responder, 2))
	input, _ := op.GetInput()
	output := make(chan any, 16)
	require.NoError(t, op.AddOutput(output, "output"))
	op.Exec(ctx, make(chan error, 4))

	row := func(channel, emitter string, ts time.Time) *checkpoint.BufferOrEvent {
		return &checkpoint.BufferOrEvent{Channel: channel, Data: &xsql.Tuple{
			Emitter:   emitter,
			Message:   map[string]interface{}{"a": ts.UnixMilli()},
			Timestamp: ts,
		}}
	}
	barrier := func(channel string, id int64) *checkpoint.BufferOrEvent {
		return &checkpoint.BufferOrEvent{Channel: channel, Data: &checkpoint.Barrier{CheckpointId: id, OpId: channel}}
	}

	input <- row("left", "demo1", base)
	input <- row("right", "demo2", base.Add(time.Second))
	// The watermark needs both streams before the first row is released.
	first, ok := receiveUnwrappedOutput(t, output).(*xsql.Tuple)
	require.True(t, ok, "first output must be a tuple, got %#v", first)
	require.Equal(t, base, first.Timestamp)

	input <- barrier("left", 1)
	input <- row("left", "demo1", base.Add(2*time.Second))
	time.Sleep(100 * time.Millisecond)
	requireNoOutput(t, output)

	// Completing C1 fires the trigger; draining the held row advances the
	// left stream watermark, which releases the right row. That release
	// proves the held row reached the operator after the trigger.
	input <- barrier("right", 1)
	require.Eventually(t, func() bool {
		return len(responder.triggers()) == 1
	}, 2*time.Second, time.Millisecond)
	second, ok := receiveUnwrappedOutput(t, output).(*xsql.Tuple)
	require.True(t, ok, "held row must advance the watermark after the trigger, got %#v", second)
	require.Equal(t, base.Add(time.Second), second.Timestamp)
	requireNoOutput(t, output)
	cancel()
}

// TestWindowOpDualInputBacklogDrain proves the v1 window consumption loop
// drains the aligner backlog: a row held during ExactlyOnce alignment must
// be emitted after the trigger fires. Graph topologies allow windows with
// multiple inputs, so this loop must consult NextDue like any other
// multi-input operator.
func TestWindowOpDualInputBacklogDrain(t *testing.T) {
	base := time.Unix(100, 0)
	options := &def.RuleOption{BufferLength: 16}
	config := node.WindowConfig{
		Type:   ast.NOT_WINDOW,
		Length: time.Second,
	}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	op, err := node.NewWindowOp("window", config, options)
	require.NoError(t, err)
	op.SetQos(def.ExactlyOnce)
	op.AddInputCount()
	op.AddInputCount()
	responder := &dualInputResponder{}
	op.SetBarrierHandler(checkpoint.NewBarrierAligner(responder, 2))
	input, _ := op.GetInput()
	output := make(chan any, 16)
	require.NoError(t, op.AddOutput(output, "output"))
	op.Exec(ctx, make(chan error, 4))

	row := func(channel string, a int64, ts time.Time) *checkpoint.BufferOrEvent {
		return &checkpoint.BufferOrEvent{Channel: channel, Data: &xsql.Tuple{
			Emitter:   "demo",
			Message:   map[string]interface{}{"a": a},
			Timestamp: ts,
		}}
	}
	barrier := func(channel string, id int64) *checkpoint.BufferOrEvent {
		return &checkpoint.BufferOrEvent{Channel: channel, Data: &checkpoint.Barrier{CheckpointId: id, OpId: channel}}
	}

	input <- row("left", 1, base)
	first, ok := receiveUnwrappedOutput(t, output).(*xsql.WindowTuples)
	require.True(t, ok, "first emission must be a window")
	require.Len(t, first.Content, 1)

	input <- barrier("left", 1)
	input <- row("left", 2, base.Add(time.Second))
	time.Sleep(100 * time.Millisecond)
	requireNoOutput(t, output)

	input <- barrier("right", 1)
	require.Eventually(t, func() bool {
		return len(responder.triggers()) == 1
	}, 2*time.Second, time.Millisecond)
	second, ok := receiveUnwrappedOutput(t, output).(*xsql.WindowTuples)
	require.True(t, ok, "second emission must be a window")
	require.NotEmpty(t, second.Content)
	found := false
	for _, r := range second.Content {
		if m, ok := r.ToMap()["a"]; ok && m == int64(2) {
			found = true
		}
	}
	require.True(t, found, "held row was lost: %#v", second.ToMaps())
	requireNoOutput(t, output)
	cancel()
}
