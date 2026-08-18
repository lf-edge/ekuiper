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

package planner

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func findWindowPlan(p LogicalPlan) *WindowPlan {
	if w, ok := p.(*WindowPlan); ok {
		return w
	}
	for _, c := range p.Children() {
		if r := findWindowPlan(c); r != nil {
			return r
		}
	}
	return nil
}

func findFilterPlan(p LogicalPlan) *FilterPlan {
	if fp, ok := p.(*FilterPlan); ok {
		return fp
	}
	for _, c := range p.Children() {
		if r := findFilterPlan(c); r != nil {
			return r
		}
	}
	return nil
}

func setupVehicleStatusStream(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	s, err := json.Marshal(&xsql.StreamInfo{
		StreamType: ast.TypeStream,
		Statement:  `CREATE STREAM vehicle_status (soc BIGINT, charge_status string, ts BIGINT) WITH (DATASOURCE="vehicle_status", FORMAT="json", TIMESTAMP="ts");`,
	})
	require.NoError(t, err)
	require.NoError(t, kv.Set("vehicle_status", string(s)))
}

// TestStateWindowWhereNotPushedDown verifies that a WHERE clause on a state
// window is pushed down into the WindowPlan (e.g. as collectCondition/condition).
func TestStateWindowWhereNotPushedDown(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT collect(*) AS charge_data FROM vehicle_status WHERE soc % 10 = 0 AND changed_col(true, soc % 10 = 0) GROUP BY statewindow(charge_status = 'charging', charge_status = 'discharging')`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, &def.RuleOption{BufferLength: 1024}, kv)
	require.NoError(t, err)

	explain, err := ExplainFromLogicalPlan(p, "charge_monitor")
	require.NoError(t, err)
	fmt.Println("==== EXPLAIN ====\n" + explain)

	// Standalone FilterPlan should be gone since condition was pushed into WindowPlan.
	require.Nil(t, findFilterPlan(p), "expected no standalone FilterPlan after pushdown")

	w := findWindowPlan(p)
	require.NotNil(t, w)
	require.True(t, w.condition != nil || w.collectCondition != nil, "state window should carry pushed-down condition")
}

// TestStateWindowWhereAfterRuntime verifies end-to-end runtime evaluation
// with the WHERE condition pushed into the state window.
//
//	t1 soc=5  charge_status=charging    -> begin window
//	t2 soc=10 charge_status=charging    -> collected
//	t3 soc=15 charge_status=discharging  -> emit window
//
// Expected: the window collects and filters, producing only the row with soc % 10 == 0, i.e. [soc=10].
func TestStateWindowWhereAfterRuntime(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT collect(*) AS charge_data FROM vehicle_status WHERE soc % 10 = 0 GROUP BY statewindow(charge_status = 'charging', charge_status = 'discharging')`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	o := &def.RuleOption{BufferLength: 1024}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)

	wp := findWindowPlan(p)
	require.NotNil(t, wp)
	require.Nil(t, findFilterPlan(p), "expected no standalone FilterPlan after pushdown")
	require.True(t, wp.condition != nil || wp.collectCondition != nil, "state window should carry pushed-down condition")

	filterCond := wp.collectCondition
	if filterCond == nil {
		filterCond = wp.condition
	}

	winOp, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:             wp.WindowType(),
		BeginCondition:   wp.GetBeginCondition(),
		EmitCondition:    wp.GetEmitCondition(),
		CollectCondition: filterCond,
	}, o)
	require.NoError(t, err)

	output := make(chan any, 10)
	require.NoError(t, winOp.AddOutput(output, "output"))

	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	errCh := make(chan error, 10)
	winOp.Exec(ctx, errCh)
	time.Sleep(50 * time.Millisecond)

	now := time.Now()
	winIn, _ := winOp.GetInput()
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(5), "charge_status": "charging"}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(10), "charge_status": "charging"}, Timestamp: now.Add(1 * time.Second)}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(15), "charge_status": "discharging"}, Timestamp: now.Add(2 * time.Second)}
	time.Sleep(100 * time.Millisecond)

	select {
	case got := <-output:
		wt, ok := got.(*xsql.WindowTuples)
		require.True(t, ok, "expected *xsql.WindowTuples, got %T", got)
		maps := wt.ToMaps()
		require.Equal(t, []map[string]any{
			{"soc": int64(10), "charge_status": "charging"},
		}, maps)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for window output")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
	winOp.Close()
}

// TestCountWindowCollectCondition verifies that a WHERE on a count window acts
// as an in-window collect filter: counting semantics (msgCount) still see every
// row while only matching rows are buffered and emitted.
func TestCountWindowCollectCondition(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY countwindow(2)`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	o := &def.RuleOption{BufferLength: 1024}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)

	wp := findWindowPlan(p)
	require.NotNil(t, wp)
	require.Nil(t, findFilterPlan(p), "expected no standalone FilterPlan after pushdown")
	require.NotNil(t, wp.collectCondition)

	winOp, err := node.NewWindowOp("window", node.WindowConfig{
		Type:             wp.WindowType(),
		CountLength:      wp.length,
		CountInterval:    wp.interval,
		CollectCondition: wp.collectCondition,
	}, o)
	require.NoError(t, err)

	output := make(chan any, 10)
	require.NoError(t, winOp.AddOutput(output, "output"))

	ctx, cancel := mockContext.NewMockContext("1", "3").WithCancel()
	errCh := make(chan error, 10)
	winOp.Exec(ctx, errCh)
	time.Sleep(50 * time.Millisecond)

	now := time.Now()
	winIn, _ := winOp.GetInput()
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(5), "charge_status": "charging"}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(10), "charge_status": "charging"}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(20), "charge_status": "discharging"}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(7), "charge_status": "discharging"}, Timestamp: now}
	time.Sleep(100 * time.Millisecond)

	select {
	case got := <-output:
		wt, ok := got.(*xsql.WindowTuples)
		require.True(t, ok, "expected *xsql.WindowTuples, got %T", got)
		maps := wt.ToMaps()
		require.Equal(t, []map[string]any{
			{"soc": int64(10), "charge_status": "charging"},
		}, maps)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for window output")
	}

	select {
	case got := <-output:
		wt, ok := got.(*xsql.WindowTuples)
		require.True(t, ok, "expected *xsql.WindowTuples, got %T", got)
		maps := wt.ToMaps()
		require.Equal(t, []map[string]any{
			{"soc": int64(20), "charge_status": "discharging"},
		}, maps)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for window output")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
	winOp.Close()
}

// TestSlidingWindowCollectCondition verifies that a WHERE on a processing-time
// sliding window (v2) only buffers matching rows. Regression test: the Sliding
// window previously bypassed the collect filter and silently dropped the WHERE.
func TestSlidingWindowCollectCondition(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY slidingwindow(ss, 1)`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	o := &def.RuleOption{BufferLength: 1024}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)

	wp := findWindowPlan(p)
	require.NotNil(t, wp)
	require.Nil(t, findFilterPlan(p), "expected no standalone FilterPlan after pushdown")
	require.NotNil(t, wp.collectCondition)

	winOp, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:             wp.WindowType(),
		Length:           1 * time.Second,
		RawInterval:      wp.interval,
		TimeUnit:         wp.timeUnit,
		CollectCondition: wp.collectCondition,
	}, o)
	require.NoError(t, err)

	output := make(chan any, 10)
	require.NoError(t, winOp.AddOutput(output, "output"))

	ctx, cancel := mockContext.NewMockContext("1", "4").WithCancel()
	errCh := make(chan error, 10)
	winOp.Exec(ctx, errCh)
	time.Sleep(50 * time.Millisecond)

	now := time.Now()
	winIn, _ := winOp.GetInput()
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(10), "charge_status": "charging"}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(3), "charge_status": "discharging"}, Timestamp: now.Add(10 * time.Millisecond)}
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 2; i++ {
		select {
		case got := <-output:
			wt, ok := got.(*xsql.WindowTuples)
			require.True(t, ok, "expected *xsql.WindowTuples, got %T", got)
			for _, m := range wt.ToMaps() {
				require.NotEqual(t, int64(3), m["soc"], "collect condition should filter out soc=3")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for window output")
		}
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
	winOp.Close()
}

// TestEventWindowCollectCondition verifies that a WHERE on an event-time sliding
// window only buffers matching rows.
func TestEventWindowCollectCondition(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY slidingwindow(ss, 10)`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	o := &def.RuleOption{BufferLength: 1024, IsEventTime: true}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)

	wp := findWindowPlan(p)
	require.NotNil(t, wp)
	require.Nil(t, findFilterPlan(p), "expected no standalone FilterPlan after pushdown")
	require.NotNil(t, wp.collectCondition)

	winOp, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:             wp.WindowType(),
		Length:           10 * time.Second,
		RawInterval:      wp.interval,
		TimeUnit:         wp.timeUnit,
		CollectCondition: wp.collectCondition,
	}, o)
	require.NoError(t, err)

	output := make(chan any, 10)
	require.NoError(t, winOp.AddOutput(output, "output"))

	ctx, cancel := mockContext.NewMockContext("1", "5").WithCancel()
	errCh := make(chan error, 10)
	winOp.Exec(ctx, errCh)
	time.Sleep(50 * time.Millisecond)

	now := time.Now()
	winIn, _ := winOp.GetInput()
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(10), "charge_status": "charging", "ts": now.UnixMilli()}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(3), "charge_status": "discharging", "ts": now.Add(10 * time.Millisecond).UnixMilli()}, Timestamp: now.Add(10 * time.Millisecond)}
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 2; i++ {
		select {
		case got := <-output:
			wt, ok := got.(*xsql.WindowTuples)
			require.True(t, ok, "expected *xsql.WindowTuples, got %T", got)
			for _, m := range wt.ToMaps() {
				require.NotEqual(t, int64(3), m["soc"], "collect condition should filter out soc=3")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for window output")
		}
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
	winOp.Close()
}
