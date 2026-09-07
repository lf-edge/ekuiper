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

package operator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestLeadErrorRecovery(t *testing.T) {
	store, _ := state.CreateStore("review", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("review", "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.FieldRef{Name: "v"}}, WhenExpr: &ast.FieldRef{Name: "b"}}}}
	row := func(v int, b any) *xsql.Tuple { return &xsql.Tuple{Message: xsql.Message{"v": v, "b": b}} }
	require.Nil(t, op.Apply(ctx, row(1, true), fv, afv))
	require.Error(t, op.Apply(ctx, row(2, "invalid"), fv, afv).(error))
	require.Len(t, op.Apply(ctx, row(3, true), fv, afv), 1)
	require.Len(t, op.Apply(ctx, row(4, true), fv, afv), 1, "a malformed row must not permanently block subsequent valid rows")
}

func TestLeadUntilIndexedProbe(t *testing.T) {
	store, _ := state.CreateStore("review-index", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("review-index", "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	field := &ast.FieldRef{Name: "ts", HasIndex: true, SourceIndex: 0}
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: 0, CachedField: "next", Args: []ast.Expr{field}, WhenExpr: &ast.BooleanLiteral{Val: false}, UntilExpr: &ast.BinaryExpr{LHS: &ast.BinaryExpr{LHS: field, OP: ast.SUB, RHS: &ast.Call{Name: "current_row", Args: []ast.Expr{field}}}, OP: ast.GT, RHS: &ast.IntegerLiteral{Val: 5}}}}}
	require.Nil(t, op.Apply(ctx, &xsql.SliceTuple{SourceContent: model.SliceVal{int64(0)}}, fv, afv))
	require.Len(t, op.Apply(ctx, &xsql.SliceTuple{SourceContent: model.SliceVal{int64(6)}}, fv, afv), 1, "UNTIL must resolve an indexed probe field")
}

func TestLeadUntilBeforeCandidate(t *testing.T) {
	store, _ := state.CreateStore("review-order", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("review-order", "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.BinaryExpr{LHS: &ast.FieldRef{Name: "v"}, OP: ast.ADD, RHS: &ast.IntegerLiteral{Val: 1}}}, WhenExpr: &ast.BooleanLiteral{Val: true}, UntilExpr: &ast.BooleanLiteral{Val: true}}}}
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"v": 1}}, fv, afv))
	require.Len(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"v": "bad"}}, fv, afv), 1, "expired requests must resolve before evaluating an unusable candidate")
}

func TestLeadUntilBeforeWhen(t *testing.T) {
	store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.FieldRef{Name: "v"}}, WhenExpr: &ast.FieldRef{Name: "b"}, UntilExpr: &ast.BooleanLiteral{Val: true}}}}
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"v": 1, "b": true}}, fv, afv))
	ready := op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"v": 2, "b": "bad"}}, fv, afv).([]xsql.Row)
	require.Len(t, ready, 1)
	value, _ := ready[0].Value("next", "")
	require.Nil(t, value)
}

func TestLeadOffsetAndNullProgress(t *testing.T) {
	store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{}
	for id, ignoreNull := range []bool{true, false} {
		name := []string{"skip_null", "count_null"}[id]
		op.Funcs = append(op.Funcs, &ast.Call{Name: "lead", FuncId: id, CacheIndex: -1, CachedField: name, Args: []ast.Expr{
			&ast.FieldRef{Name: "v"}, &ast.IntegerLiteral{Val: 2}, &ast.IntegerLiteral{Val: -1}, &ast.BooleanLiteral{Val: ignoreNull},
		}})
	}
	row := func(v any) *xsql.Tuple { return &xsql.Tuple{Message: xsql.Message{"v": v}} }
	for _, value := range []any{1, nil, 3} {
		require.Nil(t, op.Apply(ctx, row(value), fv, afv))
	}
	ready := op.Apply(ctx, row(4), fv, afv).([]xsql.Row)
	require.Len(t, ready, 2)
	for i, expected := range [][2]int{{4, 3}, {4, 4}} {
		for j, name := range []string{"skip_null", "count_null"} {
			value, _ := ready[i].Value(name, "")
			require.Equal(t, expected[j], value)
		}
	}
	tail := op.Finalize(ctx, xsql.EOFTuple("done"), fv, afv).([]xsql.Row)
	require.Len(t, tail, 2)
	for _, row := range tail {
		for _, name := range []string{"skip_null", "count_null"} {
			value, _ := row.Value(name, "")
			require.Equal(t, int64(-1), value)
		}
	}
}

func TestLeadMultipleCallsErrorRecovery(t *testing.T) {
	store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	newCall := func(id int, name string) *ast.Call {
		return &ast.Call{Name: "lead", FuncId: id, CacheIndex: -1, CachedField: name, Args: []ast.Expr{&ast.FieldRef{Name: "v"}}}
	}
	first, second := newCall(0, "first"), newCall(1, "second")
	second.WhenExpr = &ast.FieldRef{Name: "b"}
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{first, second}}
	row := func(v int, b any) *xsql.Tuple { return &xsql.Tuple{Message: xsql.Message{"v": v, "b": b}} }
	require.Nil(t, op.Apply(ctx, row(1, true), fv, afv))
	_, failed := op.Apply(ctx, row(2, "bad"), fv, afv).(error)
	require.True(t, failed)
	ready := op.Apply(ctx, row(3, true), fv, afv).([]xsql.Row)
	require.Len(t, ready, 1)
	for _, name := range []string{"first", "second"} {
		value, _ := ready[0].Value(name, "")
		require.Equal(t, 3, value, "neither call may consume the rejected probe")
	}
	tail := op.Finalize(ctx, xsql.EOFTuple("done"), fv, afv).([]xsql.Row)
	require.Len(t, tail, 1)
}

func TestLeadUnaryWatermarkAndEOF(t *testing.T) {
	store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
	runCtx, cancel := ctx.WithCancel()
	defer cancel()
	op := node.New("lead", &def.RuleOption{BufferLength: 16, SendError: true})
	op.SetOperation(&AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.FieldRef{Name: "v"}}}}})
	out, errCh := make(chan any, 16), make(chan error, 1)
	require.NoError(t, op.AddOutput(out, "test"))
	op.Exec(runCtx, errCh)
	input, _ := op.GetInput()
	base := time.Unix(100, 0)
	input <- &xsql.Tuple{Timestamp: base, Message: xsql.Message{"v": 1}}
	input <- &xsql.WatermarkTuple{Timestamp: base.Add(time.Second)}
	input <- &xsql.WatermarkTuple{Timestamp: base.Add(2 * time.Second)}
	input <- &xsql.Tuple{Timestamp: base.Add(3 * time.Second), Message: xsql.Message{"v": 2}}
	input <- &xsql.WatermarkTuple{Timestamp: base.Add(4 * time.Second)}
	input <- xsql.EOFTuple("done")
	receive := func() any {
		select {
		case result := <-out:
			return result
		case err := <-errCh:
			t.Fatalf("operator failed: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for operator output")
		}
		return nil
	}
	require.Equal(t, base.Add(-time.Nanosecond), receive().(*xsql.WatermarkTuple).Timestamp)
	first := receive().(xsql.Row)
	value, _ := first.Value("next", "")
	require.Equal(t, 2, value)
	require.Equal(t, base.Add(3*time.Second-time.Nanosecond), receive().(*xsql.WatermarkTuple).Timestamp)
	tail := receive().(xsql.Row)
	value, _ = tail.Value("next", "")
	require.Nil(t, value)
	require.Equal(t, xsql.EOFTuple("done"), receive())
}

func TestLeadWatermarkRestore(t *testing.T) {
	store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	newOp := func() *AnalyticFuncsOp {
		return &AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.FieldRef{Name: "v"}}}}}
	}
	op := newOp()
	base := time.Unix(100, 0)
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Timestamp: base, Message: xsql.Message{"v": 1}}, fv, afv))
	wm, err := op.Watermark(ctx, &xsql.WatermarkTuple{Timestamp: base.Add(time.Second)})
	require.NoError(t, err)
	require.Equal(t, base.Add(-time.Nanosecond), wm.Timestamp)
	restored := newOp()
	wm, err = restored.Watermark(ctx, &xsql.WatermarkTuple{Timestamp: base.Add(2 * time.Second)})
	require.NoError(t, err)
	require.Nil(t, wm, "restoring must preserve the watermark hold and suppress duplicates")
	require.Len(t, restored.Apply(ctx, &xsql.Tuple{Timestamp: base.Add(3 * time.Second), Message: xsql.Message{"v": 2}}, fv, afv), 1)
	wm, err = restored.Watermark(ctx, &xsql.WatermarkTuple{Timestamp: base.Add(4 * time.Second)})
	require.NoError(t, err)
	require.Equal(t, base.Add(3*time.Second-time.Nanosecond), wm.Timestamp)
}
