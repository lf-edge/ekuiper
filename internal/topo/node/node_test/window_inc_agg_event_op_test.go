// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestIncEventHoppingWindowState(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		IsEventTime:  true,
		Qos:          0,
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
	now := time.Time{}.Add(3100 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	waitExecute()
	op2, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		Length:      2 * time.Second,
		Interval:    time.Second,
		RawInterval: 1,
		TimeUnit:    ast.SS,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op2)
	input2, _ := op2.GetInput()
	output2 := make(chan any, 10)
	op2.AddOutput(output2, "output")
	op2.Exec(ctx, errCh)
	waitExecute()
	input2 <- &xsql.WatermarkTuple{Timestamp: now.Add(5 * time.Second)}
	waitExecute()
	got1 := <-output2
	wt, ok := got1.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	waitExecute()
	got2 := <-output2
	wt, ok = got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
	op2.Close()
}

func TestIncEventHoppingWindow(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		IsEventTime:  true,
		Qos:          0,
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
	time.Sleep(10 * time.Millisecond)
	now := time.Time{}.Add(3100 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	input <- &xsql.WatermarkTuple{Timestamp: now.Add(5 * time.Second)}
	got1 := <-output
	wt, ok := got1.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	got2 := <-output
	wt, ok = got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncEventSlidingWindow(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		BufferLength: 10,
		IsEventTime:  true,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,10)"
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
	waitExecute()
	tuple1Ts := time.Now()
	tuple2Ts := tuple1Ts.Add(500 * time.Millisecond)
	waterMark1Ts := tuple1Ts.Add(1200 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: tuple1Ts}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: tuple2Ts}
	input <- &xsql.WatermarkTuple{Timestamp: waterMark1Ts}
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
			"a":             int64(2),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncEventDelaySlidingWindow(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		BufferLength: 10,
		IsEventTime:  true,
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
	time.Sleep(10 * time.Millisecond)

	tuple1Ts := time.Now()
	tuple2Ts := tuple1Ts.Add(500 * time.Millisecond)
	waterMark1Ts := tuple1Ts.Add(1100 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: tuple1Ts}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: tuple2Ts}
	input <- &xsql.WatermarkTuple{Timestamp: waterMark1Ts}
	got1 := <-output
	wt, ok := got1.(*xsql.WindowTuples)
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

func TestIncEventTumblingWindow(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		IsEventTime:  true,
		Qos:          0,
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
	time.Sleep(10 * time.Millisecond)
	now := time.Time{}.Add(3100 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	input <- &xsql.WatermarkTuple{Timestamp: now.Add(5 * time.Second)}
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
			"a":             int64(2),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	time.Sleep(10 * time.Millisecond)
	op.Close()
}

func TestIncEventCountWindow(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		IsEventTime:  true,
		Qos:          0,
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by countwindow(1)"
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
	waitExecute()
	now := time.Now()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	input <- &xsql.WatermarkTuple{Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
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
	input <- &xsql.WatermarkTuple{Timestamp: now.Add(1500 * time.Millisecond)}
	waitExecute()
	got2 := <-output
	wt, ok = got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestTestIncEventSlidingWindowState(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		BufferLength: 10,
		IsEventTime:  true,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,10)"
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
	tuple1Ts := time.Now()
	tuple2Ts := tuple1Ts.Add(500 * time.Millisecond)
	waterMark1Ts := tuple1Ts.Add(1200 * time.Millisecond)
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: tuple1Ts}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: tuple2Ts}
	waitExecute()

	op2, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:   incPlan.WType,
		Length: time.Second,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input2, _ := op2.GetInput()
	output2 := make(chan any, 10)
	op2.AddOutput(output2, "output")
	op2.Exec(ctx, errCh)
	waitExecute()

	input2 <- &xsql.WatermarkTuple{Timestamp: waterMark1Ts}
	got1 := <-output2
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
	got2 := <-output2
	wt, ok = got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(2),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
	op2.Close()
}

func TestIncEventCountWindowState(t *testing.T) {
	conf.IsTesting = true
	o := &def.RuleOption{
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			EnableIncrementalWindow: true,
		},
		IsEventTime:  true,
		Qos:          0,
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by countwindow(1)"
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
	waitExecute()
	now := time.Now()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	waitExecute()
	op2, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
		Type:        incPlan.WType,
		CountLength: incPlan.Length,
	}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
	require.NoError(t, err)
	require.NotNil(t, op2)
	input2, _ := op2.GetInput()
	output2 := make(chan any, 10)
	op2.AddOutput(output2, "output")
	op2.Exec(ctx, errCh)
	waitExecute()

	input2 <- &xsql.WatermarkTuple{Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	got1 := <-output2
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
	input2 <- &xsql.WatermarkTuple{Timestamp: now.Add(1500 * time.Millisecond)}
	waitExecute()
	got2 := <-output2
	wt, ok = got2.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d = wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a":             int64(2),
			"inc_agg_col_1": int64(1),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
	op2.Close()
}
