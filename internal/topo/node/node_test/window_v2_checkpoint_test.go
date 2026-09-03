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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type windowV2CheckpointContext struct {
	api.StreamContext
	stateSaved chan struct{}
	waitGroup  sync.WaitGroup
}

func (c *windowV2CheckpointContext) PutState(key string, value interface{}) error {
	err := c.StreamContext.PutState(key, value)
	if err == nil && key == node.V2WindowInputsKey {
		select {
		case c.stateSaved <- struct{}{}:
		default:
		}
	}
	return err
}

func (c *windowV2CheckpointContext) Value(key interface{}) interface{} {
	if key == topoContext.RuleWaitGroupKey {
		return &c.waitGroup
	}
	return c.StreamContext.Value(key)
}

func newWindowV2CheckpointContext(t *testing.T, state map[string]interface{}) (*windowV2CheckpointContext, context.CancelFunc) {
	t.Helper()
	raw, cancel := mockContext.NewMockContext("window_v2_checkpoint", "window").WithCancel()
	for key, value := range state {
		require.NoError(t, raw.PutState(key, value))
	}
	return &windowV2CheckpointContext{
		StreamContext: raw,
		stateSaved:    make(chan struct{}, 16),
	}, cancel
}

func waitWindowV2StateSaved(t *testing.T, ctx *windowV2CheckpointContext) {
	t.Helper()
	select {
	case <-ctx.stateSaved:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Window V2 state")
	}
}

func waitWindowV2Processed(t *testing.T, op *node.WindowV2Operator, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		metrics := op.GetMetrics()
		return len(metrics) > 2 && metrics[2] == want
	}, time.Second, time.Millisecond)
}

func stopWindowV2Operator(t *testing.T, ctx *windowV2CheckpointContext, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	done := make(chan struct{})
	go func() {
		ctx.waitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out stopping Window V2 operator")
	}
}

func freezeWindowV2State(t *testing.T, ctx *windowV2CheckpointContext) []byte {
	t.Helper()
	stateCtx, ok := ctx.StreamContext.(interface {
		GetAllState() map[string]interface{}
	})
	require.True(t, ok)
	frozen, err := checkpoint.EncodeState(stateCtx.GetAllState())
	require.NoError(t, err)
	return frozen
}

func decodeWindowV2State(t *testing.T, frozen []byte) map[string]interface{} {
	t.Helper()
	state, err := checkpoint.DecodeState(frozen)
	require.NoError(t, err)
	return state
}

func receiveWindowV2Output(t *testing.T, output <-chan any) *xsql.WindowTuples {
	t.Helper()
	select {
	case got := <-output:
		window, ok := got.(*xsql.WindowTuples)
		require.True(t, ok)
		return window
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Window V2 output")
		return nil
	}
}

func requireNoWindowV2Output(t *testing.T, output <-chan any) {
	t.Helper()
	select {
	case got := <-output:
		t.Fatalf("unexpected Window V2 output: %#v", got)
	default:
	}
}

func newWindowV2Operator(t *testing.T, config node.WindowConfig, options *def.RuleOption, ctx api.StreamContext) (*node.WindowV2Operator, chan any, chan any) {
	t.Helper()
	op, err := node.NewWindowV2Op("window", config, options)
	require.NoError(t, err)
	input, _ := op.GetInput()
	output := make(chan any, 16)
	require.NoError(t, op.AddOutput(output, "output"))
	op.Exec(ctx, make(chan error, 4))
	return op, input, output
}

func triggerOnOne() ast.Expr {
	return &ast.BinaryExpr{
		OP: ast.EQ,
		LHS: &ast.FieldRef{
			StreamName: ast.DefaultStream,
			Name:       "a",
		},
		RHS: &ast.IntegerLiteral{Val: 1},
	}
}

func TestWindowV2SlidingCheckpointRestore(t *testing.T) {
	base := time.Unix(100, 0)
	previousClock := timex.Clock
	mockclock.ResetClock(base.UnixMilli())
	defer func() {
		timex.Clock = previousClock
	}()

	options := &def.RuleOption{BufferLength: 16}
	config := node.WindowConfig{
		Type:   ast.SLIDING_WINDOW,
		Length: 10 * time.Second,
	}
	ctx1, cancel1 := newWindowV2CheckpointContext(t, nil)
	op1, input1, output1 := newWindowV2Operator(t, config, options, ctx1)
	waitWindowV2StateSaved(t, ctx1)
	input1 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(1)},
		Timestamp: base,
	}
	_ = receiveWindowV2Output(t, output1)
	waitWindowV2Processed(t, op1, 1)
	frozen := freezeWindowV2State(t, ctx1)

	live, err := ctx1.GetState(node.V2WindowInputsKey)
	require.NoError(t, err)
	liveState := live.(*node.SlidingWindowV2State)
	liveState.Scanner.Tuples[0].Message["a"] = int64(99)
	stopWindowV2Operator(t, ctx1, cancel1)

	restored := decodeWindowV2State(t, frozen)
	restoredState := restored[node.V2WindowInputsKey].(*node.SlidingWindowV2State)
	require.NotSame(t, liveState, restoredState)
	require.NotSame(t, liveState.Scanner, restoredState.Scanner)
	require.Equal(t, int64(1), restoredState.Scanner.Tuples[0].Message["a"])

	ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
	op2, input2, output2 := newWindowV2Operator(t, config, options, ctx2)
	waitWindowV2StateSaved(t, ctx2)
	input2 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(2)},
		Timestamp: base.Add(time.Second),
	}
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
		{"a": int64(2)},
	}, receiveWindowV2Output(t, output2).ToMaps())
	waitWindowV2Processed(t, op2, 1)
	stopWindowV2Operator(t, ctx2, cancel2)
}

func TestWindowV2SlidingDelayCheckpointRestore(t *testing.T) {
	base := time.Unix(200, 0)
	previousClock := timex.Clock
	mockclock.ResetClock(base.UnixMilli())
	defer func() {
		timex.Clock = previousClock
	}()

	options := &def.RuleOption{BufferLength: 16}
	config := node.WindowConfig{
		Type:             ast.SLIDING_WINDOW,
		Delay:            5 * time.Second,
		Length:           10 * time.Second,
		TriggerCondition: triggerOnOne(),
	}
	ctx1, cancel1 := newWindowV2CheckpointContext(t, nil)
	op1, input1, output1 := newWindowV2Operator(t, config, options, ctx1)
	waitWindowV2StateSaved(t, ctx1)
	input1 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(1)},
		Timestamp: base,
	}
	waitWindowV2Processed(t, op1, 1)
	timex.Add(2 * time.Second)
	input1 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(2)},
		Timestamp: base.Add(2 * time.Second),
	}
	waitWindowV2Processed(t, op1, 2)
	requireNoWindowV2Output(t, output1)
	frozen := freezeWindowV2State(t, ctx1)

	live, err := ctx1.GetState(node.V2WindowInputsKey)
	require.NoError(t, err)
	liveState := live.(*node.SlidingWindowV2State)
	require.Len(t, liveState.Pending, 1)
	liveState.Scanner.Tuples[0].Message["a"] = int64(99)
	stopWindowV2Operator(t, ctx1, cancel1)

	t.Run("future deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(4 * time.Second).UnixMilli())
		restored := decodeWindowV2State(t, frozen)
		restoredState := restored[node.V2WindowInputsKey].(*node.SlidingWindowV2State)
		require.NotSame(t, liveState, restoredState)
		require.Equal(t, int64(1), restoredState.Scanner.Tuples[0].Message["a"])

		ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
		_, _, output2 := newWindowV2Operator(t, config, options, ctx2)
		waitWindowV2StateSaved(t, ctx2)
		timex.Add(999 * time.Millisecond)
		requireNoWindowV2Output(t, output2)
		timex.Add(time.Millisecond)
		window := receiveWindowV2Output(t, output2)
		require.Equal(t, []map[string]interface{}{
			{"a": int64(1)},
			{"a": int64(2)},
		}, window.ToMaps())
		state, stateErr := ctx2.GetState(node.V2WindowInputsKey)
		require.NoError(t, stateErr)
		require.Empty(t, state.(*node.SlidingWindowV2State).Pending)
		timex.Add(10 * time.Second)
		requireNoWindowV2Output(t, output2)
		stopWindowV2Operator(t, ctx2, cancel2)
	})

	t.Run("overdue deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(6 * time.Second).UnixMilli())
		restored := decodeWindowV2State(t, frozen)
		ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
		_, _, output2 := newWindowV2Operator(t, config, options, ctx2)
		waitWindowV2StateSaved(t, ctx2)
		require.Equal(t, []map[string]interface{}{
			{"a": int64(1)},
			{"a": int64(2)},
		}, receiveWindowV2Output(t, output2).ToMaps())
		state, stateErr := ctx2.GetState(node.V2WindowInputsKey)
		require.NoError(t, stateErr)
		require.Empty(t, state.(*node.SlidingWindowV2State).Pending)
		timex.Add(10 * time.Second)
		requireNoWindowV2Output(t, output2)
		stopWindowV2Operator(t, ctx2, cancel2)
	})
}

func TestEventSlidingWindowV2CheckpointRestore(t *testing.T) {
	base := time.Unix(300, 0)
	options := &def.RuleOption{
		BufferLength: 16,
		IsEventTime:  true,
	}
	config := node.WindowConfig{
		Type:             ast.SLIDING_WINDOW,
		Delay:            5 * time.Second,
		Length:           10 * time.Second,
		TriggerCondition: triggerOnOne(),
	}
	ctx1, cancel1 := newWindowV2CheckpointContext(t, nil)
	op1, input1, output1 := newWindowV2Operator(t, config, options, ctx1)
	waitWindowV2StateSaved(t, ctx1)
	input1 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(1)},
		Timestamp: base.Add(10 * time.Second),
	}
	waitWindowV2Processed(t, op1, 1)
	input1 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(2)},
		Timestamp: base.Add(12 * time.Second),
	}
	waitWindowV2Processed(t, op1, 2)
	requireNoWindowV2Output(t, output1)
	frozen := freezeWindowV2State(t, ctx1)

	live, err := ctx1.GetState(node.V2WindowInputsKey)
	require.NoError(t, err)
	liveState := live.(*node.EventSlidingWindowV2State)
	require.Len(t, liveState.DelayTS, 1)
	liveState.Scanner.Tuples[0].Message["a"] = int64(99)
	stopWindowV2Operator(t, ctx1, cancel1)

	restored := decodeWindowV2State(t, frozen)
	restoredState := restored[node.V2WindowInputsKey].(*node.EventSlidingWindowV2State)
	require.NotSame(t, liveState, restoredState)
	require.NotSame(t, liveState.Scanner, restoredState.Scanner)
	require.Equal(t, int64(1), restoredState.Scanner.Tuples[0].Message["a"])

	ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
	op2, input2, output2 := newWindowV2Operator(t, config, options, ctx2)
	waitWindowV2StateSaved(t, ctx2)
	input2 <- &xsql.WatermarkTuple{Timestamp: base.Add(14 * time.Second)}
	input2 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(3)},
		Timestamp: base.Add(14 * time.Second),
	}
	waitWindowV2Processed(t, op2, 1)
	requireNoWindowV2Output(t, output2)
	input2 <- &xsql.WatermarkTuple{Timestamp: base.Add(15 * time.Second)}
	input2 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(5)},
		Timestamp: base.Add(15 * time.Second),
	}
	waitWindowV2Processed(t, op2, 2)
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
		{"a": int64(2)},
		{"a": int64(3)},
	}, receiveWindowV2Output(t, output2).ToMaps())
	state, stateErr := ctx2.GetState(node.V2WindowInputsKey)
	require.NoError(t, stateErr)
	require.Empty(t, state.(*node.EventSlidingWindowV2State).DelayTS)

	input2 <- &xsql.WatermarkTuple{Timestamp: base.Add(16 * time.Second)}
	input2 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(4)},
		Timestamp: base.Add(16 * time.Second),
	}
	waitWindowV2Processed(t, op2, 3)
	requireNoWindowV2Output(t, output2)
	stopWindowV2Operator(t, ctx2, cancel2)
}
