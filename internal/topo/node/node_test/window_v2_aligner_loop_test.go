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

// loopResponder records checkpoint triggers fired through a real operator
// loop. It performs no broadcast or snapshot: the test only observes
// trigger timing relative to window emissions.
type loopResponder struct {
	mu    sync.Mutex
	fired []int64
	// onFire runs inside TriggerCheckpoint, i.e. synchronously where the
	// snapshot would be taken, so it observes exactly what a snapshot
	// would capture.
	onFire func(id int64)
}

func (r *loopResponder) TriggerCheckpoint(id int64) error {
	if r.onFire != nil {
		r.onFire(id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fired = append(r.fired, id)
	return nil
}

func (r *loopResponder) GetName() string {
	return "window"
}

func (r *loopResponder) triggers() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.fired...)
}

func loopBarrier(channel string, id int64) *checkpoint.BufferOrEvent {
	return &checkpoint.BufferOrEvent{Channel: channel, Data: &checkpoint.Barrier{CheckpointId: id, OpId: channel}}
}

func loopRow(channel string, a int64, ts time.Time) *checkpoint.BufferOrEvent {
	return &checkpoint.BufferOrEvent{Channel: channel, Data: &xsql.Tuple{
		Message:   map[string]interface{}{"a": a},
		Timestamp: ts,
	}}
}

// TestWindowV2LoopDrainsAlignerBacklog proves the backlog is wired into the
// production consumption path, not just the handler internals: a row held
// during ExactlyOnce alignment must be emitted after the trigger fires,
// through the real operator loop with its timer and control cases intact.
// Against an unwired replay this row is lost and the test times out.
func TestWindowV2LoopDrainsAlignerBacklog(t *testing.T) {
	base := time.Unix(100, 0)
	options := &def.RuleOption{BufferLength: 16}
	config := node.WindowConfig{
		Type:   ast.SLIDING_WINDOW,
		Length: 10 * time.Second,
	}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	// Mirror the production wiring order: QoS, input counts and the barrier
	// handler are all in place before the operator loop starts.
	op, err := node.NewWindowV2Op("window", config, options)
	require.NoError(t, err)
	op.SetQos(def.ExactlyOnce)
	op.AddInputCount()
	op.AddInputCount()
	responder := &loopResponder{}
	op.SetBarrierHandler(checkpoint.NewBarrierAligner(responder, 2))
	input, _ := op.GetInput()
	output := make(chan any, 16)
	require.NoError(t, op.AddOutput(output, "output"))
	op.Exec(ctx, make(chan error, 4))
	waitWindowV2StateSaved(t, ctx)

	input <- loopRow("left", 1, base)
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
	}, receiveWindowV2BarrierOutput(t, output).ToMaps())
	waitWindowV2Processed(t, op, 1)

	// Align C1 on the left; the next left row must be held, not emitted.
	// Held rows bypass onProcessStart, so the records-in metric cannot prove
	// consumption here; a short settle wait plus the exact output sequence
	// below carries the proof instead.
	input <- loopBarrier("left", 1)
	input <- loopRow("left", 2, base.Add(time.Second))
	time.Sleep(100 * time.Millisecond)
	requireNoWindowV2Output(t, output)

	// Complete C1 from the right: the trigger fires, then the loop drains
	// the held row through the normal emission path.
	input <- loopBarrier("right", 1)
	require.Eventually(t, func() bool {
		return len(responder.triggers()) == 1
	}, time.Second, time.Millisecond)
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
		{"a": int64(2)},
	}, receiveWindowV2BarrierOutput(t, output).ToMaps())
	waitWindowV2Processed(t, op, 2)
	requireNoWindowV2Output(t, output)
	stopWindowV2Operator(t, ctx, cancel)
}

// TestWindowV2LoopAbortReplaysBeforeNewSnapshot drives C1 preemption
// through a real operator loop: the row held while aligning C1 must be
// fully processed before the C2 snapshot fires after C2 completes. The
// responder inspects the live window state at trigger time, proving the
// snapshot would have captured the replayed row.
func TestWindowV2LoopAbortReplaysBeforeNewSnapshot(t *testing.T) {
	base := time.Unix(100, 0)
	options := &def.RuleOption{BufferLength: 16}
	config := node.WindowConfig{
		Type:   ast.SLIDING_WINDOW,
		Length: 10 * time.Second,
	}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	op, err := node.NewWindowV2Op("window", config, options)
	require.NoError(t, err)
	op.SetQos(def.ExactlyOnce)
	op.AddInputCount()
	op.AddInputCount()
	responder := &loopResponder{}
	var atTrigger []map[string]interface{}
	responder.onFire = func(id int64) {
		v, err := ctx.GetState(node.V2WindowInputsKey)
		if err != nil {
			return
		}
		state, ok := v.(*node.SlidingWindowV2State)
		if !ok || state.Scanner == nil {
			return
		}
		for _, tuple := range state.Scanner.Tuples {
			atTrigger = append(atTrigger, map[string]interface{}(tuple.Message))
		}
	}
	op.SetBarrierHandler(checkpoint.NewBarrierAligner(responder, 2))
	input, _ := op.GetInput()
	output := make(chan any, 16)
	require.NoError(t, op.AddOutput(output, "output"))
	op.Exec(ctx, make(chan error, 4))
	waitWindowV2StateSaved(t, ctx)

	input <- loopRow("left", 1, base)
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
	}, receiveWindowV2BarrierOutput(t, output).ToMaps())
	waitWindowV2Processed(t, op, 1)

	// Align C1 on the left, hold the next row, then let C2 preempt it from
	// the right. Neither checkpoint may trigger yet.
	input <- loopBarrier("left", 1)
	input <- loopRow("left", 2, base.Add(time.Second))
	time.Sleep(100 * time.Millisecond)
	requireNoWindowV2Output(t, output)
	input <- loopBarrier("right", 2)
	// The abort releases the held row for immediate replay through the
	// normal emission path, still before any C2 trigger exists.
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
		{"a": int64(2)},
	}, receiveWindowV2BarrierOutput(t, output).ToMaps())
	require.Empty(t, responder.triggers())

	// Complete C2 from the left. The positioned trigger fires with the
	// replayed row already reflected in the window state.
	input <- loopBarrier("left", 2)
	require.Eventually(t, func() bool {
		return len(responder.triggers()) == 1
	}, 2*time.Second, time.Millisecond)
	require.Equal(t, []int64{2}, responder.triggers())
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
		{"a": int64(2)},
	}, atTrigger)
	requireNoWindowV2Output(t, output)
	stopWindowV2Operator(t, ctx, cancel)
}
