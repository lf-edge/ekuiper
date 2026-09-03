// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	testx.InitEnv("node_test")
}

func TestWindowState(t *testing.T) {
	conf.IsTesting = true
	node.EnableAlignWindow = false
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	testcases := []struct {
		sql string
	}{
		{
			sql: "select count(*) from stream group by tumblingWindow(ss,1)",
		},
		{
			sql: "select count(*) from stream group by slidingWindow(ss,1)",
		},
		{
			sql: "select count(*) from stream group by hoppingWindow(ss,2,1)",
		},
	}
	for _, tt := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		require.NoError(t, err)
		p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
				EnableIncrementalWindow: true,
			},
			Qos: 0,
		}, kv)
		require.NoError(t, err)
		require.NotNil(t, p)
		incPlan := extractIncWindowPlan(p)
		require.NotNil(t, incPlan)
		op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
			Type:     incPlan.WType,
			Length:   time.Second,
			Interval: time.Second,
		}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
		require.NoError(t, err)
		require.NotNil(t, op)
		input, _ := op.GetInput()
		output := make(chan any, 10)
		op.AddOutput(output, "output")
		errCh := make(chan error, 10)
		ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
		op.Exec(ctx, errCh)
		time.Sleep(10 * time.Millisecond)
		input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, op.PutState4Test(ctx))

		op2, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
			Type:     incPlan.WType,
			Length:   time.Second,
			Interval: time.Second,
		}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
		require.NoError(t, err)
		require.NotNil(t, op2)
		op2.Exec(ctx, errCh)
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, op2.RestoreFromState4Test(ctx))
		cancel()
		op.Close()
		op2.Close()
	}
}

func TestIncAggCountWindowState(t *testing.T) {
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by countwindow(2)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		CountLength: incPlan.Length,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	time.Sleep(10 * time.Millisecond)

	op2, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		CountLength: incPlan.Length,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	input, _ = op2.GetInput()
	output = make(chan any, 10)
	op2.AddOutput(output, "output")
	errCh = make(chan error, 10)
	op2.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
	op2.Close()
}

func TestIncAggWindow(t *testing.T) {
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by countwindow(2)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		CountLength: incPlan.Length,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncAggAlignTumblingWindow(t *testing.T) {
	conf.IsTesting = true
	node.EnableAlignWindow = true
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by tumblingWindow(ss,1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		RawInterval: 1,
		TimeUnit:    ast.SS,
		Interval:    time.Second,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	defer func() {
		cancel()
	}()
	op.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	require.Eventually(t, func() bool {
		return op.FirstTimerCreated4Test()
	}, time.Second, 10*time.Millisecond)
}

func TestIncAggTumblingWindow(t *testing.T) {
	conf.IsTesting = true
	node.EnableAlignWindow = false
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by tumblingWindow(ss,1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:     incPlan.WType,
		Interval: time.Second,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	waitExecute()
	timex.Add(1100 * time.Millisecond)
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncAggSlidingWindow(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,100)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:   incPlan.WType,
		Length: time.Second,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	got1 := <-output
	wt, ok := got1.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	got2 := <-output
	wt, ok = got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncAggSlidingWindowRestoreFromDecodedState(t *testing.T) {
	conf.IsTesting = true
	incPlan := buildIncAggPlan(t, "select count(*) from stream group by slidingWindow(ss,100)")
	options := &def.RuleOption{BufferLength: 10}
	config := &node.WindowConfig{
		Type:   incPlan.WType,
		Length: time.Minute,
	}

	ctx, cancel := newIncAggTestContext("decode_rule", "sliding")
	op, input, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	first := receiveIncAggWindow(t, output, errCh)
	require.Equal(t, int64(1), first.ToMaps()[0]["inc_agg_col_1"])
	require.NoError(t, op.PutState4Test(ctx))

	frozen := freezeIncAggState(t, ctx)
	stopIncAggOperator(t, ctx, cancel)
	restoredCtx, restoredCancel := decodedIncAggContext(t, frozen, "decode_rule", "sliding")

	restoredOp, err := node.NewWindowIncAggOp("checkpoint_test", config, incPlan.Dimensions, incPlan.IncAggFuncs, options)
	require.NoError(t, err)
	sliding, ok := restoredOp.WindowExec.(*node.SlidingWindowIncAggOp)
	require.True(t, ok)
	require.NoError(t, sliding.RestoreFromState(restoredCtx))

	restoredInput, _ := restoredOp.GetInput()
	restoredOutput := make(chan any, 2)
	require.NoError(t, restoredOp.AddOutput(restoredOutput, "output"))
	restoredErrCh := make(chan error, 2)
	restoredOp.Exec(restoredCtx, restoredErrCh)
	restoredInput <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}}
	second := receiveIncAggWindow(t, restoredOutput, restoredErrCh)
	require.Equal(t, int64(2), second.ToMaps()[0]["inc_agg_col_1"])
	stopIncAggOperator(t, restoredCtx, restoredCancel)
}

func TestProcessingIncAggSlidingDelayCheckpointDeadlines(t *testing.T) {
	base := time.Unix(1_000, 0)
	previousClock := timex.Clock
	previousAlign := node.EnableAlignWindow
	mockclock.ResetClock(base.UnixMilli())
	node.EnableAlignWindow = false
	defer func() {
		timex.Clock = previousClock
		node.EnableAlignWindow = previousAlign
	}()

	incPlan := buildIncAggPlan(t, "select count(*) from stream group by slidingWindow(ss,10,5)")
	options := &def.RuleOption{BufferLength: 16}
	config := &node.WindowConfig{
		Type:   incPlan.WType,
		Length: 10 * time.Second,
		Delay:  5 * time.Second,
	}
	ctx1, cancel1 := newIncAggTestContext("sliding_timer", "op")
	op1, input1, output1, errCh1 := startIncAggTestOperator(t, incPlan, config, options, ctx1)
	input1 <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	waitForIncAggProcessed(t, op1, 1)
	waitForIncAggState(t, op1, ctx1, func(value any) bool {
		state, ok := value.(node.SlidingWindowIncAggOpState)
		return ok && len(state.Pending) == 1 && len(state.CurrWindowList) == 1
	})
	requireNoIncAggOutput(t, output1, errCh1)
	timex.Add(2 * time.Second)
	frozen := freezeIncAggState(t, ctx1)
	stopIncAggOperator(t, ctx1, cancel1)

	t.Run("future deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(4 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, frozen, "sliding_timer", "op")
		op, _, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		timex.Add(999 * time.Millisecond)
		requireNoIncAggOutput(t, output, errCh)
		timex.Add(time.Millisecond)
		window := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(1), window.ToMaps()[0]["inc_agg_col_1"])
		waitForIncAggState(t, op, ctx, func(value any) bool {
			state, ok := value.(node.SlidingWindowIncAggOpState)
			return ok && len(state.Pending) == 0
		})
		timex.Add(10 * time.Second)
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})

	t.Run("overdue deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(6 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, frozen, "sliding_timer", "op")
		op, _, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		window := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(1), window.ToMaps()[0]["inc_agg_col_1"])
		waitForIncAggState(t, op, ctx, func(value any) bool {
			state, ok := value.(node.SlidingWindowIncAggOpState)
			return ok && len(state.Pending) == 0
		})
		timex.Add(10 * time.Second)
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})

	t.Run("multiple overdue deadlines", func(t *testing.T) {
		multiBase := base.Add(100 * time.Second)
		mockclock.ResetClock(multiBase.UnixMilli())
		ctx1, cancel1 := newIncAggTestContext("sliding_multi_timer", "op")
		op1, input1, output1, errCh1 := startIncAggTestOperator(t, incPlan, config, options, ctx1)
		input1 <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
		waitForIncAggProcessed(t, op1, 1)
		timex.Add(time.Second)
		input1 <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}}
		waitForIncAggProcessed(t, op1, 2)
		waitForIncAggState(t, op1, ctx1, func(value any) bool {
			state, ok := value.(node.SlidingWindowIncAggOpState)
			return ok && len(state.Pending) == 2
		})
		requireNoIncAggOutput(t, output1, errCh1)
		multiFrozen := freezeIncAggState(t, ctx1)
		stopIncAggOperator(t, ctx1, cancel1)

		mockclock.ResetClock(multiBase.Add(20 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, multiFrozen, "sliding_multi_timer", "op")
		op, input, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		input <- &xsql.Tuple{Message: map[string]any{"a": int64(3)}}
		first := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(2), first.ToMaps()[0]["inc_agg_col_1"])
		firstEnd, ok := first.FuncValue("window_end")
		require.True(t, ok)
		require.Equal(t, multiBase.Add(5*time.Second).UnixMilli(), firstEnd)
		second := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(2), second.ToMaps()[0]["inc_agg_col_1"])
		secondEnd, ok := second.FuncValue("window_end")
		require.True(t, ok)
		require.Equal(t, multiBase.Add(6*time.Second).UnixMilli(), secondEnd)
		waitForIncAggProcessed(t, op, 1)
		waitForIncAggState(t, op, ctx, func(value any) bool {
			state, ok := value.(node.SlidingWindowIncAggOpState)
			return ok && len(state.Pending) == 1 &&
				state.Pending[0].FireAt.Equal(multiBase.Add(25*time.Second)) &&
				len(state.CurrWindowList) == 1
		})
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})
}

func TestProcessingIncAggTumblingCheckpointDeadlines(t *testing.T) {
	base := time.Unix(2_000, 0)
	previousClock := timex.Clock
	previousAlign := node.EnableAlignWindow
	mockclock.ResetClock(base.UnixMilli())
	node.EnableAlignWindow = false
	defer func() {
		timex.Clock = previousClock
		node.EnableAlignWindow = previousAlign
	}()

	incPlan := buildIncAggPlan(t, "select count(*) from stream group by tumblingWindow(ss,5)")
	options := &def.RuleOption{BufferLength: 16}
	config := &node.WindowConfig{
		Type:        incPlan.WType,
		Interval:    5 * time.Second,
		RawInterval: 5,
		TimeUnit:    ast.SS,
	}
	ctx1, cancel1 := newIncAggTestContext("tumbling_timer", "op")
	op1, input1, _, _ := startIncAggTestOperator(t, incPlan, config, options, ctx1)
	input1 <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	waitForIncAggProcessed(t, op1, 1)
	waitForIncAggState(t, op1, ctx1, func(value any) bool {
		state, ok := value.(node.TumblingWindowIncAggOpState)
		return ok && state.CurrWindow != nil && state.NextTriggerTime.Equal(base.Add(5*time.Second))
	})
	timex.Add(2 * time.Second)
	frozen := freezeIncAggState(t, ctx1)
	stopIncAggOperator(t, ctx1, cancel1)

	t.Run("future deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(4 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, frozen, "tumbling_timer", "op")
		_, _, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		timex.Add(999 * time.Millisecond)
		requireNoIncAggOutput(t, output, errCh)
		timex.Add(time.Millisecond)
		window := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(1), window.ToMaps()[0]["inc_agg_col_1"])
		timex.Add(4999 * time.Millisecond)
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})

	t.Run("overdue deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(6 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, frozen, "tumbling_timer", "op")
		op, _, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		window := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(1), window.ToMaps()[0]["inc_agg_col_1"])
		waitForIncAggState(t, op, ctx, func(value any) bool {
			state, ok := value.(node.TumblingWindowIncAggOpState)
			return ok && state.CurrWindow == nil && state.NextTriggerTime.Equal(base.Add(10*time.Second))
		})
		timex.Add(3999 * time.Millisecond)
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})
}

func TestProcessingIncAggHoppingCheckpointDeadlines(t *testing.T) {
	base := time.Unix(3_000, 0)
	previousClock := timex.Clock
	previousAlign := node.EnableAlignWindow
	mockclock.ResetClock(base.UnixMilli())
	node.EnableAlignWindow = false
	defer func() {
		timex.Clock = previousClock
		node.EnableAlignWindow = previousAlign
	}()

	incPlan := buildIncAggPlan(t, "select count(*) from stream group by hoppingWindow(ss,5,2)")
	options := &def.RuleOption{BufferLength: 16}
	config := &node.WindowConfig{
		Type:        incPlan.WType,
		Length:      5 * time.Second,
		Interval:    2 * time.Second,
		RawInterval: 2,
		TimeUnit:    ast.SS,
	}
	ctx1, cancel1 := newIncAggTestContext("hopping_timer", "op")
	op1, input1, _, _ := startIncAggTestOperator(t, incPlan, config, options, ctx1)
	input1 <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	waitForIncAggProcessed(t, op1, 1)
	waitForIncAggState(t, op1, ctx1, func(value any) bool {
		state, ok := value.(node.HoppingWindowIncAggOpState)
		return ok && len(state.CurrWindowList) == 1 &&
			len(state.CurrWindowList[0].DimensionsIncAggRange) == 1 &&
			state.NextWindowTime.Equal(base.Add(2*time.Second))
	})
	timex.Add(time.Second)
	frozen := freezeIncAggState(t, ctx1)
	stopIncAggOperator(t, ctx1, cancel1)

	t.Run("future deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(4 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, frozen, "hopping_timer", "op")
		op, _, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		waitForIncAggState(t, op, ctx, func(value any) bool {
			state, ok := value.(node.HoppingWindowIncAggOpState)
			return ok && state.NextWindowTime.Equal(base.Add(6*time.Second))
		})
		timex.Add(999 * time.Millisecond)
		requireNoIncAggOutput(t, output, errCh)
		timex.Add(time.Millisecond)
		window := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(1), window.ToMaps()[0]["inc_agg_col_1"])
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})

	t.Run("overdue deadline", func(t *testing.T) {
		mockclock.ResetClock(base.Add(6 * time.Second).UnixMilli())
		ctx, cancel := decodedIncAggContext(t, frozen, "hopping_timer", "op")
		op, _, output, errCh := startIncAggTestOperator(t, incPlan, config, options, ctx)
		window := receiveIncAggWindow(t, output, errCh)
		require.Equal(t, int64(1), window.ToMaps()[0]["inc_agg_col_1"])
		waitForIncAggState(t, op, ctx, func(value any) bool {
			state, ok := value.(node.HoppingWindowIncAggOpState)
			return ok && state.NextWindowTime.Equal(base.Add(8*time.Second))
		})
		timex.Add(999 * time.Millisecond)
		requireNoIncAggOutput(t, output, errCh)
		stopIncAggOperator(t, ctx, cancel)
	})
}

func TestIncAggSlidingWindowOver(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,100) over(when a > 1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:             incPlan.WType,
		Length:           time.Second,
		TriggerCondition: incPlan.TriggerCondition,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}}
	got2 := <-output
	wt, ok := got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncAggSlidingWindowDelay(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,1,1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:   incPlan.WType,
		Length: time.Second,
		Delay:  time.Second,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	waitExecute()
	timex.Add(500 * time.Millisecond)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}}
	waitExecute()
	timex.Add(600 * time.Millisecond)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(3)}}
	waitExecute()
	timex.Add(2 * time.Second)
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	got = <-output
	wt, ok = got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(3),
			"inc_agg_col_1": int64(3),
		},
	}, d)
	got = <-output
	wt, ok = got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(3),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func waitExecute() {
	time.Sleep(50 * time.Millisecond)
}

func TestIncHoppingWindow(t *testing.T) {
	conf.IsTesting = true
	node.EnableAlignWindow = false
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by hoppingWindow(ss,2,1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		Length:      2 * time.Second,
		Interval:    time.Second,
		RawInterval: 1,
		TimeUnit:    ast.SS,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}}
	waitExecute()
	timex.Add(2200 * time.Millisecond)
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(1),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncAggAlignHoppingWindow(t *testing.T) {
	conf.IsTesting = true
	node.EnableAlignWindow = true
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by hoppingWindow(ss,2,1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	incPlan := extractIncWindowPlan(p)
	require.NotNil(t, incPlan)
	op, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		RawInterval: 1,
		TimeUnit:    ast.SS,
		Length:      2 * time.Second,
		Interval:    time.Second,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	defer func() {
		cancel()
	}()
	op.Exec(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	require.Eventually(t, func() bool {
		return op.FirstTimerCreated4Test()
	}, time.Second, 10*time.Millisecond)
}

func extractIncWindowPlan(cur planner.LogicalPlan) *planner.IncWindowPlan {
	switch plan := cur.(type) {
	case *planner.IncWindowPlan:
		return plan
	default:
		for _, child := range plan.Children() {
			got := extractIncWindowPlan(child)
			if got != nil {
				return got
			}
		}
	}
	return nil
}

func buildIncAggPlan(t *testing.T, sql string) *planner.IncWindowPlan {
	t.Helper()
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	plan, err := planner.CreateLogicalPlan(stmt, &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	incPlan := extractIncWindowPlan(plan)
	require.NotNil(t, incPlan)
	return incPlan
}

func startIncAggTestOperator(
	t *testing.T,
	incPlan *planner.IncWindowPlan,
	config *node.WindowConfig,
	options *def.RuleOption,
	ctx api.StreamContext,
) (*node.WindowIncAggOperator, chan any, chan any, chan error) {
	t.Helper()
	op, err := node.NewWindowIncAggOp("checkpoint_test", config, incPlan.Dimensions, incPlan.IncAggFuncs, options)
	require.NoError(t, err)
	input, _ := op.GetInput()
	output := make(chan any, 16)
	require.NoError(t, op.AddOutput(output, "output"))
	errCh := make(chan error, 4)
	op.Exec(ctx, errCh)
	return op, input, output, errCh
}

func freezeIncAggState(t *testing.T, ctx api.StreamContext) []byte {
	t.Helper()
	key := incAggStateKey(ctx)
	stateValue, err := ctx.GetState(key)
	require.NoError(t, err)
	require.NotNil(t, stateValue)
	frozen, err := checkpoint.EncodeState(map[string]interface{}{key: stateValue})
	require.NoError(t, err)
	return frozen
}

func restoreIncAggState(t *testing.T, frozen []byte, ctx api.StreamContext) {
	t.Helper()
	key := incAggStateKey(ctx)
	decoded, err := checkpoint.DecodeState(frozen)
	require.NoError(t, err)
	require.NoError(t, ctx.PutState(key, decoded[key]))
}

func incAggStateKey(ctx api.StreamContext) string {
	return fmt.Sprintf("%v_%v_%v/state", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}

func waitForIncAggState(
	t *testing.T,
	op *node.WindowIncAggOperator,
	ctx api.StreamContext,
	ready func(any) bool,
) {
	t.Helper()
	require.NoError(t, op.PutState4Test(ctx))
	key := incAggStateKey(ctx)
	stateValue, err := ctx.GetState(key)
	require.NoError(t, err)
	require.NotNil(t, stateValue)
	require.True(t, ready(stateValue))
}

func waitForIncAggProcessed(t *testing.T, op *node.WindowIncAggOperator, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		metrics := op.GetMetrics()
		return len(metrics) > 2 && metrics[2] == want
	}, time.Second, time.Millisecond)
}

func requireNoIncAggOutput(t *testing.T, output <-chan any, errCh <-chan error) {
	t.Helper()
	select {
	case value := <-output:
		t.Fatalf("unexpected incremental window output: %#v", value)
	case err := <-errCh:
		t.Fatalf("incremental window failed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

type incAggTestContext struct {
	api.StreamContext
	waitGroup sync.WaitGroup
}

func (c *incAggTestContext) Value(key interface{}) interface{} {
	if key == topoContext.RuleWaitGroupKey {
		return &c.waitGroup
	}
	return c.StreamContext.Value(key)
}

func newIncAggTestContext(ruleID, opID string) (*incAggTestContext, context.CancelFunc) {
	ctx, cancel := mockContext.NewMockContext(ruleID, opID).WithCancel()
	return &incAggTestContext{StreamContext: ctx}, cancel
}

func stopIncAggOperator(t *testing.T, ctx *incAggTestContext, cancel context.CancelFunc) {
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
		t.Fatal("timed out stopping incremental window operator")
	}
}

func decodedIncAggContext(t *testing.T, frozen []byte, ruleID, opID string) (*incAggTestContext, context.CancelFunc) {
	t.Helper()
	ctx, cancel := newIncAggTestContext(ruleID, opID)
	restoreIncAggState(t, frozen, ctx)
	return ctx, cancel
}

func receiveIncAggWindow(t *testing.T, output <-chan any, errCh <-chan error) *xsql.WindowTuples {
	t.Helper()
	select {
	case value := <-output:
		window, ok := value.(*xsql.WindowTuples)
		require.True(t, ok, "unexpected output type %T", value)
		return window
	case err := <-errCh:
		t.Fatalf("incremental window failed: %v", err)
		return nil
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for incremental window output")
		return nil
	}
}

func prepareStream() error {
	kv, err := store.GetKV("stream")
	if err != nil {
		return err
	}
	streamSqls := map[string]string{
		"sharedStream": `CREATE STREAM sharedStream (
					a BIGINT,
					b BIGINT,
				) WITH (DATASOURCE="src1", SHARED="true");`,
		"stream": `CREATE STREAM stream (
					a BIGINT,
					b BIGINT,
				) WITH (DATASOURCE="src1");`,
		"eventStream": `CREATE STREAM eventStream (
					a  BIGINT,
					b  BIGINT,
                    ts BIGINT
				) WITH (DATASOURCE="src1",TIMESTAMP="ts");`,
	}

	types := map[string]ast.StreamType{
		"sharedStream": ast.TypeStream,
		"stream":       ast.TypeStream,
		"eventStream":  ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			return err
		}
		err = kv.Set(name, string(s))
		if err != nil {
			return err
		}
	}
	return nil
}
