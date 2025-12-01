// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	testx.InitEnv("node_test")
}

func TestStateWindowStatePartition(t *testing.T) {
	conf.IsTesting = true
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by statewindow(a =1 , a = 2) over (partition by b)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:           windowPlan.WindowType(),
		BeginCondition: windowPlan.GetBeginCondition(),
		EmitCondition:  windowPlan.GetEmitCondition(),
		PartitionExpr:  windowPlan.GetPartitionExpr(),
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": true}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": false}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1), "b": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1), "b": int64(2)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2), "b": int64(1)}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(1),
			"b": int64(1),
		},
		{
			"a": int64(2),
			"b": int64(1),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestStateWindowState(t *testing.T) {
	conf.IsTesting = true
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by statewindow(a)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:            windowPlan.WindowType(),
		SingleCondition: windowPlan.GetSingleCondition(),
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": true}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": false}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	op2, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:            windowPlan.WindowType(),
		SingleCondition: windowPlan.GetSingleCondition(),
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op2)
	input2, _ := op2.GetInput()
	output2 := make(chan any, 10)
	op2.AddOutput(output2, "output")
	errCh2 := make(chan error, 10)
	op2.Exec(ctx, errCh2)
	waitExecute()
	input2 <- &xsql.Tuple{Message: map[string]any{"a": false}, Timestamp: now.Add(500 * time.Millisecond)}
	input2 <- &xsql.Tuple{Message: map[string]any{"a": true}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	got := <-output2
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": true,
		},
		{
			"a": false,
		},
		{
			"a": false,
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
	op2.Close()
}

func TestSingleConditionStWindow(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by statewindow(a)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:            windowPlan.WindowType(),
		SingleCondition: windowPlan.GetSingleCondition(),
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": true}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": false}, Timestamp: now.Add(500 * time.Millisecond)}
	input <- &xsql.Tuple{Message: map[string]any{"a": false}, Timestamp: now.Add(500 * time.Millisecond)}
	input <- &xsql.Tuple{Message: map[string]any{"a": true}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": true,
		},
		{
			"a": false,
		},
		{
			"a": false,
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestStateWindow(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by statewindow(a>1,a >5)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:           windowPlan.WindowType(),
		BeginCondition: windowPlan.GetBeginCondition(),
		EmitCondition:  windowPlan.GetEmitCondition(),
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(500 * time.Millisecond)}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(6)}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(2),
		},
		{
			"a": int64(6),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestWindowV2SlidingWindowDelay(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,1,1) over (when a = 1)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		TriggerCondition: windowPlan.GetTriggerCondition(),
		Type:             windowPlan.WindowType(),
		Delay:            time.Second,
		Length:           time.Second,
		RawInterval:      1,
		TimeUnit:         ast.SS,
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(500 * time.Millisecond)}
	waitExecute()
	timex.Add(2 * time.Second)
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(1),
		},
		{
			"a": int64(2),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestWindowV2SlidingWindowCondition(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,4) over (when a = 2)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		TriggerCondition: windowPlan.GetTriggerCondition(),
		Type:             windowPlan.WindowType(),
		Length:           4 * time.Second,
		RawInterval:      4,
		TimeUnit:         ast.SS,
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(2)}, Timestamp: now.Add(time.Second)}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(1),
		},
		{
			"a": int64(2),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}

func TestWindowV2SlidingWindow(t *testing.T) {
	conf.IsTesting = true
	now := time.Now()
	o := &def.RuleOption{
		BufferLength: 10,
	}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := "select count(*) from stream group by slidingWindow(ss,4)"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := planner.CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)
	require.NotNil(t, p)
	windowPlan := extractWindowPlan(p)
	require.NotNil(t, windowPlan)
	op, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:        windowPlan.WindowType(),
		Length:      4 * time.Second,
		RawInterval: 4,
		TimeUnit:    ast.SS,
	}, o)
	require.NoError(t, err)
	require.NotNil(t, op)
	input, _ := op.GetInput()
	output := make(chan any, 10)
	op.AddOutput(output, "output")
	errCh := make(chan error, 10)
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	op.Exec(ctx, errCh)
	waitExecute()
	input <- &xsql.Tuple{Message: map[string]any{"a": int64(1)}, Timestamp: now}
	waitExecute()
	got := <-output
	wt, ok := got.(*xsql.WindowTuples)
	require.True(t, ok)
	require.NotNil(t, wt)
	d := wt.ToMaps()
	require.Equal(t, []map[string]any{
		{
			"a": int64(1),
		},
	}, d)
	cancel()
	waitExecute()
	op.Close()
}
