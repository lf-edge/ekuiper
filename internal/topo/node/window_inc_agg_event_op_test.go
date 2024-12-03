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
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

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
	time.Sleep(10 * time.Millisecond)
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
