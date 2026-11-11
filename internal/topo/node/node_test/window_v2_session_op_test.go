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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// sessionWindowConfig creates a session window partitioned by field b. The
// time unit is second so that the max duration checks are aligned to it.
func sessionWindowConfig(maxDuration, timeout int) node.WindowConfig {
	return node.WindowConfig{
		Type:        ast.SESSION_WINDOW,
		Length:      time.Duration(maxDuration) * time.Second,
		Interval:    time.Duration(timeout) * time.Second,
		RawInterval: maxDuration,
		TimeUnit:    ast.SS,
		PartitionExpr: &ast.PartitionExpr{
			Exprs: []ast.Expr{&ast.FieldRef{StreamName: ast.DefaultStream, Name: "b"}},
		},
	}
}

func sessionTuple(a, b int64, ts time.Time) *xsql.Tuple {
	return &xsql.Tuple{
		Message:   map[string]interface{}{"a": a, "b": b},
		Timestamp: ts,
	}
}

func requireSessionWindow(t *testing.T, output <-chan any, start, end time.Time, rows []map[string]interface{}) {
	t.Helper()
	window := receiveWindowV2Output(t, output)
	require.Equal(t, rows, window.ToMaps())
	ws, _ := window.WindowRange.FuncValue("window_start")
	we, _ := window.WindowRange.FuncValue("window_end")
	require.Equal(t, start.UnixMilli(), ws)
	require.Equal(t, end.UnixMilli(), we)
}

func setupSessionWindowClock(t *testing.T, base time.Time) {
	previousClock := timex.Clock
	mockclock.ResetClock(base.UnixMilli())
	t.Cleanup(func() {
		timex.Clock = previousClock
	})
}

func TestSessionWindowV2PartitionTimeout(t *testing.T) {
	base := time.Unix(100, 0)
	setupSessionWindowClock(t, base)
	options := &def.RuleOption{BufferLength: 16}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	op, input, output := newWindowV2Operator(t, sessionWindowConfig(10, 2), options, ctx)
	waitWindowV2StateSaved(t, ctx)

	input <- sessionTuple(1, 1, base)
	waitWindowV2Processed(t, op, 1)
	timex.SetNow(base.Add(time.Second))
	input <- sessionTuple(2, 2, base.Add(time.Second))
	waitWindowV2Processed(t, op, 2)
	timex.SetNow(base.Add(1500 * time.Millisecond))
	// Each partition has its own timeout
	input <- sessionTuple(3, 1, base.Add(1500*time.Millisecond))
	waitWindowV2Processed(t, op, 3)
	requireNoWindowV2Output(t, output)

	timex.SetNow(base.Add(3 * time.Second))
	requireSessionWindow(t, output, base.Add(time.Second), base.Add(3*time.Second), []map[string]interface{}{
		{"a": int64(2), "b": int64(2)},
	})
	requireNoWindowV2Output(t, output)
	timex.SetNow(base.Add(3500 * time.Millisecond))
	requireSessionWindow(t, output, base, base.Add(3500*time.Millisecond), []map[string]interface{}{
		{"a": int64(1), "b": int64(1)},
		{"a": int64(3), "b": int64(1)},
	})
	state, err := ctx.GetState(node.V2WindowInputsKey)
	require.NoError(t, err)
	require.Empty(t, state.(*node.SessionWindowV2State).Sessions)

	// A new row starts a new session of the partition
	timex.SetNow(base.Add(5 * time.Second))
	input <- sessionTuple(4, 1, base.Add(5*time.Second))
	waitWindowV2Processed(t, op, 4)
	timex.SetNow(base.Add(7 * time.Second))
	requireSessionWindow(t, output, base.Add(5*time.Second), base.Add(7*time.Second), []map[string]interface{}{
		{"a": int64(4), "b": int64(1)},
	})
	stopWindowV2Operator(t, ctx, cancel)
}

func TestSessionWindowV2PartitionMaxDuration(t *testing.T) {
	// The max duration checks are at 102s, 104s, 106s...
	base := time.Unix(100, 0)
	setupSessionWindowClock(t, base)
	options := &def.RuleOption{BufferLength: 16}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	op, input, output := newWindowV2Operator(t, sessionWindowConfig(2, 1), options, ctx)
	waitWindowV2StateSaved(t, ctx)

	processed := int64(0)
	for i := 0; i < 3; i++ {
		ts := base.Add(time.Duration(i) * 800 * time.Millisecond)
		timex.SetNow(ts)
		input <- sessionTuple(int64(i), 1, ts)
		processed++
		waitWindowV2Processed(t, op, processed)
		if i == 1 {
			// A short session of another partition is not split
			input <- sessionTuple(10, 2, ts)
			processed++
			waitWindowV2Processed(t, op, processed)
		}
	}
	timex.SetNow(base.Add(2400 * time.Millisecond))
	// The session of partition 2 times out before the check
	requireSessionWindow(t, output, base.Add(800*time.Millisecond), base.Add(1800*time.Millisecond), []map[string]interface{}{
		{"a": int64(10), "b": int64(2)},
	})
	// The session of partition 1 lasts for the max duration at the check
	requireSessionWindow(t, output, base, base.Add(2*time.Second), []map[string]interface{}{
		{"a": int64(0), "b": int64(1)},
		{"a": int64(1), "b": int64(1)},
		{"a": int64(2), "b": int64(1)},
	})
	requireNoWindowV2Output(t, output)

	// The session continues after the split until it times out
	input <- sessionTuple(3, 1, base.Add(2400*time.Millisecond))
	waitWindowV2Processed(t, op, processed+1)
	timex.SetNow(base.Add(3400 * time.Millisecond))
	requireSessionWindow(t, output, base.Add(2*time.Second), base.Add(3400*time.Millisecond), []map[string]interface{}{
		{"a": int64(3), "b": int64(1)},
	})
	timex.Add(10 * time.Second)
	requireNoWindowV2Output(t, output)
	stopWindowV2Operator(t, ctx, cancel)
}

func TestSessionWindowV2CheckpointRestore(t *testing.T) {
	base := time.Unix(300, 0)
	setupSessionWindowClock(t, base)
	options := &def.RuleOption{BufferLength: 16}
	config := sessionWindowConfig(10, 2)
	ctx1, cancel1 := newWindowV2CheckpointContext(t, nil)
	op1, input1, output1 := newWindowV2Operator(t, config, options, ctx1)
	waitWindowV2StateSaved(t, ctx1)
	input1 <- sessionTuple(1, 1, base)
	waitWindowV2Processed(t, op1, 1)
	input1 <- sessionTuple(2, 2, base)
	waitWindowV2Processed(t, op1, 2)
	requireNoWindowV2Output(t, output1)
	frozen := freezeWindowV2State(t, ctx1)

	live, err := ctx1.GetState(node.V2WindowInputsKey)
	require.NoError(t, err)
	liveState := live.(*node.SessionWindowV2State)
	require.Len(t, liveState.Sessions, 2)
	stopWindowV2Operator(t, ctx1, cancel1)

	t.Run("open session", func(t *testing.T) {
		mockclock.ResetClock(base.Add(time.Second).UnixMilli())
		restored := decodeWindowV2State(t, frozen)
		restoredState := restored[node.V2WindowInputsKey].(*node.SessionWindowV2State)
		require.NotSame(t, liveState, restoredState)
		require.Len(t, restoredState.Sessions, 2)

		ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
		op2, input2, output2 := newWindowV2Operator(t, config, options, ctx2)
		waitWindowV2StateSaved(t, ctx2)
		input2 <- sessionTuple(3, 1, base.Add(time.Second))
		waitWindowV2Processed(t, op2, 1)
		requireNoWindowV2Output(t, output2)
		timex.Add(time.Second)
		requireSessionWindow(t, output2, base, base.Add(2*time.Second), []map[string]interface{}{
			{"a": int64(2), "b": int64(2)},
		})
		timex.Add(time.Second)
		requireSessionWindow(t, output2, base, base.Add(3*time.Second), []map[string]interface{}{
			{"a": int64(1), "b": int64(1)},
			{"a": int64(3), "b": int64(1)},
		})
		stopWindowV2Operator(t, ctx2, cancel2)
	})

	t.Run("expired session", func(t *testing.T) {
		mockclock.ResetClock(base.Add(5 * time.Second).UnixMilli())
		restored := decodeWindowV2State(t, frozen)
		ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
		_, _, output2 := newWindowV2Operator(t, config, options, ctx2)
		waitWindowV2StateSaved(t, ctx2)
		// Both sessions timed out during the downtime and are sent on restore
		requireSessionWindow(t, output2, base, base.Add(2*time.Second), []map[string]interface{}{
			{"a": int64(1), "b": int64(1)},
		})
		requireSessionWindow(t, output2, base, base.Add(2*time.Second), []map[string]interface{}{
			{"a": int64(2), "b": int64(2)},
		})
		state, stateErr := ctx2.GetState(node.V2WindowInputsKey)
		require.NoError(t, stateErr)
		require.Empty(t, state.(*node.SessionWindowV2State).Sessions)
		stopWindowV2Operator(t, ctx2, cancel2)
	})
}

func TestEventSessionWindowV2Partition(t *testing.T) {
	// The max duration checks are at 102s, 104s, 106s...
	base := time.Unix(100, 0)
	setupSessionWindowClock(t, base)
	options := &def.RuleOption{BufferLength: 16, IsEventTime: true}
	ctx, cancel := newWindowV2CheckpointContext(t, nil)
	op, input, output := newWindowV2Operator(t, sessionWindowConfig(2, 1), options, ctx)
	waitWindowV2StateSaved(t, ctx)

	input <- sessionTuple(1, 1, base)
	input <- sessionTuple(2, 2, base.Add(500*time.Millisecond))
	input <- sessionTuple(3, 1, base.Add(800*time.Millisecond))
	waitWindowV2Processed(t, op, 3)
	// The processing time doesn't close the sessions in event time mode
	timex.Add(time.Minute)
	requireNoWindowV2Output(t, output)

	// The watermark closes the session of partition 2 only
	input <- &xsql.WatermarkTuple{Timestamp: base.Add(1600 * time.Millisecond)}
	requireSessionWindow(t, output, base.Add(500*time.Millisecond), base.Add(1500*time.Millisecond), []map[string]interface{}{
		{"a": int64(2), "b": int64(2)},
	})
	requireNoWindowV2Output(t, output)

	input <- sessionTuple(4, 1, base.Add(1600*time.Millisecond))
	// The row after the check splits the session of partition 1
	input <- sessionTuple(5, 1, base.Add(2400*time.Millisecond))
	requireSessionWindow(t, output, base, base.Add(2*time.Second), []map[string]interface{}{
		{"a": int64(1), "b": int64(1)},
		{"a": int64(3), "b": int64(1)},
		{"a": int64(4), "b": int64(1)},
	})
	// A row after the timeout closes the session and starts a new one
	input <- sessionTuple(6, 1, base.Add(4*time.Second))
	requireSessionWindow(t, output, base.Add(2*time.Second), base.Add(3400*time.Millisecond), []map[string]interface{}{
		{"a": int64(5), "b": int64(1)},
	})
	input <- &xsql.WatermarkTuple{Timestamp: base.Add(5 * time.Second)}
	requireSessionWindow(t, output, base.Add(4*time.Second), base.Add(5*time.Second), []map[string]interface{}{
		{"a": int64(6), "b": int64(1)},
	})
	requireNoWindowV2Output(t, output)
	stopWindowV2Operator(t, ctx, cancel)
}

func TestSessionWindowV2InvalidConfig(t *testing.T) {
	_, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:     ast.SESSION_WINDOW,
		Length:   0,
		Interval: time.Second,
	}, &def.RuleOption{BufferLength: 16})
	require.EqualError(t, err, "session window requires positive max duration and timeout, but got 0s and 1s")
}
