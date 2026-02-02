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
	"encoding/json"
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

func TestWindowState(t *testing.T) {
	if testx.Race {
		t.Skip("skip race test")
	}
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
		op.WindowExec.PutState(ctx)

		op2, err := node.NewWindowIncAggOp("1", &node.WindowConfig{
			Type:     incPlan.WType,
			Interval: time.Second,
		}, incPlan.Dimensions, incPlan.IncAggFuncs, o)
		require.NoError(t, err)
		require.NotNil(t, op2)
		op2.Exec(ctx, errCh)
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, op2.WindowExec.RestoreFromState(ctx))
		cancel()
		op.Close()
		op2.Close()
	}
}

func TestIncAggCountWindowState(t *testing.T) {
	if testx.Race {
		t.Skip("skip race test")
	}
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
	if testx.Race {
		t.Skip("skip race test")
	}
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
	if testx.Race {
		t.Skip("skip race test")
	}
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
	to, ok := op.WindowExec.(*node.TumblingWindowIncAggOp)
	require.True(t, ok)
	require.NotNil(t, to.FirstTimer)
}

func TestIncAggTumblingWindow(t *testing.T) {
	if testx.Race {
		t.Skip("skip race test")
	}
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
	if testx.Race {
		t.Skip("skip race test")
	}
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

func TestIncAggSlidingWindowOver(t *testing.T) {
	if testx.Race {
		t.Skip("skip race test")
	}
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
	if testx.Race {
		t.Skip("skip race test")
	}
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
	if testx.Race {
		t.Skip("skip race test")
	}
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
	if testx.Race {
		t.Skip("skip race test")
	}
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
	ho, ok := op.WindowExec.(*node.HoppingWindowIncAggOp)
	require.True(t, ok)
	require.NotNil(t, ho.FirstTimer)
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
