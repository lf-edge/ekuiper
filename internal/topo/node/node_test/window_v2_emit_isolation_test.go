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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/operator"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// applyFieldModifyingProject runs a real ProjectOp over an emitted window,
// mimicking `SELECT a + 1 AS z, a AS x`. The expression field exercises
// Row.Set while the alias field exercises Row.AppendAlias; the projection
// itself exercises Row.Pick. All three mutate the row in place.
func applyFieldModifyingProject(t *testing.T, ctx api.StreamContext, window *xsql.WindowTuples) interface{} {
	t.Helper()
	pp := &operator.ProjectOp{}
	pp.ExprFields = append(pp.ExprFields, ast.Field{
		Name: "z",
		Expr: &ast.BinaryExpr{
			OP:  ast.ADD,
			LHS: &ast.FieldRef{StreamName: ast.DefaultStream, Name: "a"},
			RHS: &ast.IntegerLiteral{Val: 1},
		},
	})
	ar, err := ast.NewAliasRef(&ast.FieldRef{StreamName: ast.DefaultStream, Name: "a"})
	require.NoError(t, err)
	pp.AliasFields = append(pp.AliasFields, ast.Field{
		AName: "x",
		Expr:  &ast.FieldRef{StreamName: ast.AliasStream, Name: "x", AliasRef: ar},
	})
	fv, afv := xsql.NewFunctionValuersForOp(nil)
	return pp.Apply(ctx, window, fv, afv)
}

// receiveWindowV2BarrierOutput receives one window emission. With QoS at or
// above AtLeastOnce the operator broadcasts a BufferOrEvent, so unwrap it.
func receiveWindowV2BarrierOutput(t *testing.T, output <-chan any) *xsql.WindowTuples {
	t.Helper()
	select {
	case got := <-output:
		if boe, ok := got.(*checkpoint.BufferOrEvent); ok {
			got = boe.Data
		}
		window, ok := got.(*xsql.WindowTuples)
		require.True(t, ok, "expect *xsql.WindowTuples, got %#v", got)
		return window
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Window V2 output")
		return nil
	}
}

// TestWindowV2SlidingProjectBoundaryPreservesRetainedState locks in the
// window -> project isolation invariant: tuples retained in the scanner
// must not be polluted when the downstream projection mutates the emitted
// rows, otherwise a barrier snapshot and any restore would observe the
// projected data. The project paths clone rows before in-place mutation
// (see WindowTuples.RangeSet), so the retained state must stay intact.
func TestWindowV2SlidingProjectBoundaryPreservesRetainedState(t *testing.T) {
	base := time.Unix(100, 0)
	options := &def.RuleOption{BufferLength: 16}
	config := node.WindowConfig{
		Type:   ast.SLIDING_WINDOW,
		Length: 10 * time.Second,
	}
	ctx1, cancel1 := newWindowV2CheckpointContext(t, nil)
	op1, input1, output1 := newWindowV2Operator(t, config, options, ctx1)
	op1.SetQos(def.AtLeastOnce)
	waitWindowV2StateSaved(t, ctx1)
	input1 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(1)},
		Timestamp: base,
	}
	window := receiveWindowV2BarrierOutput(t, output1)
	waitWindowV2Processed(t, op1, 1)

	projected, ok := applyFieldModifyingProject(t, ctx1, window).(*xsql.WindowTuples)
	require.True(t, ok, "project must return the window, got %#v", projected)
	require.Equal(t, []map[string]interface{}{
		{"z": int64(2), "x": int64(1)},
	}, projected.ToMaps())

	live, err := ctx1.GetState(node.V2WindowInputsKey)
	require.NoError(t, err)
	liveState := live.(*node.SlidingWindowV2State)
	require.Len(t, liveState.Scanner.Tuples, 1)
	require.Equal(t, xsql.Message{"a": int64(1)}, liveState.Scanner.Tuples[0].Message)
	frozen := freezeWindowV2State(t, ctx1)
	stopWindowV2Operator(t, ctx1, cancel1)

	restored := decodeWindowV2State(t, frozen)
	restoredState := restored[node.V2WindowInputsKey].(*node.SlidingWindowV2State)
	require.Len(t, restoredState.Scanner.Tuples, 1)
	require.Equal(t, xsql.Message{"a": int64(1)}, restoredState.Scanner.Tuples[0].Message)

	ctx2, cancel2 := newWindowV2CheckpointContext(t, restored)
	op2, input2, output2 := newWindowV2Operator(t, config, options, ctx2)
	op2.SetQos(def.AtLeastOnce)
	waitWindowV2StateSaved(t, ctx2)
	input2 <- &xsql.Tuple{
		Message:   map[string]interface{}{"a": int64(2)},
		Timestamp: base.Add(time.Second),
	}
	require.Equal(t, []map[string]interface{}{
		{"a": int64(1)},
		{"a": int64(2)},
	}, receiveWindowV2BarrierOutput(t, output2).ToMaps())
	waitWindowV2Processed(t, op2, 1)
	stopWindowV2Operator(t, ctx2, cancel2)
}

type windowV2EmitBenchContext struct {
	api.StreamContext
	wg *sync.WaitGroup
}

func (c *windowV2EmitBenchContext) Value(key interface{}) interface{} {
	if key == context.RuleWaitGroupKey {
		return c.wg
	}
	return c.StreamContext.Value(key)
}

// BenchmarkWindowV2SlidingEmit drives the full ingest+emit path at steady
// state to quantify the copy-on-output cost in allocs.
func BenchmarkWindowV2SlidingEmit(b *testing.B) {
	base := time.Unix(100, 0)
	options := &def.RuleOption{BufferLength: 4096}
	config := node.WindowConfig{
		Type:   ast.SLIDING_WINDOW,
		Length: time.Minute,
	}
	raw, cancel := mockContext.NewMockContext("window_v2_emit_bench", "window").WithCancel()
	ctx := &windowV2EmitBenchContext{StreamContext: raw, wg: &sync.WaitGroup{}}
	op, err := node.NewWindowV2Op("window", config, options)
	if err != nil {
		b.Fatal(err)
	}
	op.SetQos(def.AtLeastOnce)
	input, _ := op.GetInput()
	output := make(chan any, 4096)
	if err := op.AddOutput(output, "output"); err != nil {
		b.Fatal(err)
	}
	op.Exec(ctx, make(chan error, 4))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input <- &xsql.Tuple{
			Message:   map[string]interface{}{"a": int64(i)},
			Timestamp: base.Add(time.Duration(i) * time.Second),
		}
		<-output
	}
	b.StopTimer()
	cancel()
	ctx.wg.Wait()
}
