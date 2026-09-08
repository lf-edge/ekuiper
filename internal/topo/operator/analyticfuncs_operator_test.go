// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestAnalyticFuncs(t *testing.T) {
	tests := []struct {
		funcs  []*ast.Call
		data   []interface{}
		result []any
	}{
		{ // 0 Lag test
			funcs: []*ast.Call{
				{
					Name: "lag",
					Args: []ast.Expr{
						&ast.FieldRef{Name: "a"},
					},
					FuncId:      0,
					CachedField: "$$a_lag_0",
				},
				{
					Name: "lag",
					Args: []ast.Expr{
						&ast.FieldRef{Name: "b"},
					},
					FuncId:      1,
					CachedField: "$$a_lag_1",
				},
			},
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: []any{map[string]any{
				"$$a_lag_0": nil,
				"$$a_lag_1": nil,
			}, map[string]any{
				"$$a_lag_0": "a1", "$$a_lag_1": "b1",
			}, map[string]any{
				"$$a_lag_0": "a1", "$$a_lag_1": "b2",
			}, map[string]any{
				"$$a_lag_0": "a1", "$$a_lag_1": "b2",
			}},
		},
		{ // 1 changed test
			funcs: []*ast.Call{
				{
					Name: "changed_col",
					Args: []ast.Expr{
						&ast.BooleanLiteral{Val: false},
						&ast.FieldRef{Name: "a"},
					},
					FuncId:      0,
					CachedField: "$$a_changed_col_0",
				},
				{
					Name: "lag",
					Args: []ast.Expr{
						&ast.FieldRef{Name: "b"},
					},
					FuncId:      1,
					CachedField: "$$a_lag_1",
				},
				{
					Name: "had_changed",
					Args: []ast.Expr{
						&ast.BooleanLiteral{Val: true},
						&ast.FieldRef{Name: "c"},
					},
					FuncId:      0,
					CachedField: "$$a_had_changed_0",
				},
			},
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: []any{
				map[string]any{
					"$$a_changed_col_0": "a1", "$$a_had_changed_0": false, "$$a_lag_1": nil,
				}, map[string]any{
					"$$a_changed_col_0": nil, "$$a_had_changed_0": true, "$$a_lag_1": "b1",
				}, map[string]any{
					"$$a_changed_col_0": nil, "$$a_had_changed_0": false, "$$a_lag_1": "b1",
				}, map[string]any{
					"$$a_changed_col_0": nil, "$$a_had_changed_0": true, "$$a_lag_1": "b1",
				},
			},
		},
		{ // 1 Lag slice test
			funcs: []*ast.Call{
				{
					Name: "lag",
					Args: []ast.Expr{
						&ast.FieldRef{Name: "a", HasIndex: true, SourceIndex: 2},
					},
					FuncId:      0,
					CachedField: "$$a_lag_0",
					CacheIndex:  0,
				},
				{
					Name: "lag",
					Args: []ast.Expr{
						&ast.FieldRef{Name: "b", HasIndex: true, SourceIndex: 1},
					},
					FuncId:      1,
					CachedField: "$$a_lag_1",
					CacheIndex:  1,
				},
			},
			data: []interface{}{
				&xsql.SliceTuple{SourceContent: model.SliceVal{"c1", "b1", "a1"}},
				&xsql.SliceTuple{SourceContent: model.SliceVal{"c1", "b2", "a1"}},
				&xsql.SliceTuple{SourceContent: model.SliceVal{"c1", nil, "a1"}},
				&xsql.SliceTuple{SourceContent: model.SliceVal{"c2", "b2", "a1"}},
			},
			result: []any{
				model.SliceVal{nil, nil},
				model.SliceVal{"a1", "b1"},
				model.SliceVal{"a1", "b2"},
				model.SliceVal{"a1", "b2"},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestChangedFuncs_Apply1")
	for i, tt := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			tempStore, _ := state.CreateStore("mockRule"+strconv.Itoa(i), def.AtMostOnce)
			ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("mockRule"+strconv.Itoa(i), "project", tempStore)
			pp := &AnalyticFuncsOp{Funcs: tt.funcs}
			fv, afv := xsql.NewFunctionValuersForOp(ctx)
			r := make([]any, 0, len(tt.data))
			for _, d := range tt.data {
				opResult := pp.Apply(ctx, d, fv, afv)
				switch rt := opResult.(type) {
				case *xsql.Tuple:
					r = append(r, rt.CalCols)
				case *xsql.SliceTuple:
					r = append(r, rt.TempCalContent)
				}

			}
			require.Equal(t, tt.result, r)
		})
	}
}

func TestLeadUntilPerOriginRow(t *testing.T) {
	lead := &ast.Call{
		Name:        "lead",
		FuncId:      0,
		CachedField: "$$a_lead_0",
		Args:        []ast.Expr{&ast.FieldRef{Name: "candidate"}},
		WhenExpr: &ast.BinaryExpr{
			LHS: &ast.FieldRef{Name: "b"},
			OP:  ast.EQ,
			RHS: &ast.BooleanLiteral{Val: true},
		},
		UntilExpr: &ast.BinaryExpr{
			LHS: &ast.BinaryExpr{
				LHS: &ast.FieldRef{Name: "ts"},
				OP:  ast.SUB,
				RHS: &ast.Call{Name: "current_row", Args: []ast.Expr{&ast.FieldRef{Name: "ts"}}},
			},
			OP:  ast.GT,
			RHS: &ast.IntegerLiteral{Val: 5},
		},
	}
	tempStore, _ := state.CreateStore("TestLeadUntilPerOriginRow", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("TestLeadUntilPerOriginRow", "analytic", tempStore)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{lead}}

	row := func(ts int64, b bool, candidate int64) *xsql.Tuple {
		return &xsql.Tuple{Emitter: "test", Message: xsql.Message{"ts": ts, "b": b, "candidate": candidate}}
	}
	require.Nil(t, op.Apply(ctx, row(0, false, 100), fv, afv))
	require.Nil(t, op.Apply(ctx, row(4, false, 200), fv, afv))

	result := op.Apply(ctx, row(6, true, 300), fv, afv)
	ready := result.([]xsql.Row)
	require.Len(t, ready, 2)
	value, ok := ready[0].Value("$$a_lead_0", "")
	require.True(t, ok)
	require.Nil(t, value, "the row at ts=0 must expire before matching b")
	value, ok = ready[1].Value("$$a_lead_0", "")
	require.True(t, ok)
	require.Equal(t, int64(300), value, "the same b must resolve the newer row at ts=4")

	result = op.Apply(ctx, row(12, false, 400), fv, afv)
	ready = result.([]xsql.Row)
	require.Len(t, ready, 1)
	value, ok = ready[0].Value("$$a_lead_0", "")
	require.True(t, ok)
	require.Nil(t, value, "the b row's own lookup must start after that row")

	result = op.Finalize(ctx, xsql.EOFTuple(""), fv, afv)
	ready = result.([]xsql.Row)
	require.Len(t, ready, 1)
	value, ok = ready[0].Value("$$a_lead_0", "")
	require.True(t, ok)
	require.Nil(t, value, "EOF must resolve the remaining tail with the default")
	stored, err := ctx.GetState(leadOperatorStateKey)
	require.NoError(t, err)
	require.Nil(t, stored)
}

func TestLeadMatchesAtExactUntilBoundary(t *testing.T) {
	lead := &ast.Call{
		Name:        "lead",
		FuncId:      0,
		CachedField: "$$a_lead_0",
		Args:        []ast.Expr{&ast.FieldRef{Name: "value"}},
		WhenExpr:    &ast.FieldRef{Name: "b"},
		UntilExpr: &ast.BinaryExpr{
			LHS: &ast.BinaryExpr{
				LHS: &ast.FieldRef{Name: "ts"},
				OP:  ast.SUB,
				RHS: &ast.Call{Name: "current_row", Args: []ast.Expr{&ast.FieldRef{Name: "ts"}}},
			},
			OP:  ast.GT,
			RHS: &ast.IntegerLiteral{Val: 5},
		},
	}
	tempStore, _ := state.CreateStore("TestLeadMatchesAtExactUntilBoundary", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("TestLeadMatchesAtExactUntilBoundary", "analytic", tempStore)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{lead}}
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(0), "b": false, "value": 1}}, fv, afv))
	result := op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(5), "b": true, "value": 9}}, fv, afv)
	ready := result.([]xsql.Row)
	require.Len(t, ready, 1)
	value, ok := ready[0].Value("$$a_lead_0", "")
	require.True(t, ok)
	require.Equal(t, 9, value)
}

func TestLeadRestoresPendingRows(t *testing.T) {
	newCall := func() *ast.Call {
		return &ast.Call{
			Name: "lead", FuncId: 0, CachedField: "$$a_lead_0",
			Args:     []ast.Expr{&ast.FieldRef{Name: "value"}},
			WhenExpr: &ast.FieldRef{Name: "b"},
			UntilExpr: &ast.BinaryExpr{
				LHS: &ast.BinaryExpr{LHS: &ast.FieldRef{Name: "ts"}, OP: ast.SUB, RHS: &ast.Call{
					Name: "current_row", Args: []ast.Expr{&ast.FieldRef{Name: "ts"}},
				}},
				OP: ast.GT, RHS: &ast.IntegerLiteral{Val: 5},
			},
		}
	}
	tempStore, _ := state.CreateStore("TestLeadRestoresPendingRows", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("TestLeadRestoresPendingRows", "analytic", tempStore)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{Funcs: []*ast.Call{newCall()}}
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(0), "b": false, "value": 1}}, fv, afv))
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(4), "b": false, "value": 2}}, fv, afv))
	require.NoError(t, op.Snapshot(ctx))
	stored, err := ctx.GetState(leadOperatorStateKey)
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(&stored))
	require.Contains(t, encoded.String(), "github.com/lf-edge/ekuiper/v2/internal/topo/operator.LeadOperatorState", "retain the pre-refactor checkpoint type name")
	var decoded interface{}
	require.NoError(t, gob.NewDecoder(&encoded).Decode(&decoded))
	require.IsType(t, leadSnapshot{}, decoded)
	require.NoError(t, ctx.PutState(leadOperatorStateKey, decoded))

	restored := &AnalyticFuncsOp{Funcs: []*ast.Call{newCall()}}
	result := restored.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(6), "b": true, "value": 3}}, fv, afv)
	ready := result.([]xsql.Row)
	require.Len(t, ready, 2)
	oldValue, _ := ready[0].Value("$$a_lead_0", "")
	newerValue, _ := ready[1].Value("$$a_lead_0", "")
	require.Nil(t, oldValue)
	require.Equal(t, 3, newerValue)
}

func TestEagerAnalyticAliasFeedsLead(t *testing.T) {
	latest := &ast.Call{
		Name: "latest", FuncId: 0, CachedField: "$$a_latest_0", CacheIndex: -1,
		Args: []ast.Expr{&ast.FieldRef{Name: "ts"}}, WhenExpr: &ast.FieldRef{Name: "eligible"},
	}
	alias := &ast.AliasRef{Expression: latest}
	latest.Cached = true
	lead := &ast.Call{
		Name: "lead", FuncId: 1, CachedField: "$$a_lead_1", CacheIndex: -1,
		Args:     []ast.Expr{&ast.FieldRef{Name: "candidate", StreamName: ast.AliasStream, AliasRef: alias}},
		WhenExpr: &ast.FieldRef{Name: "b"},
	}
	tempStore, _ := state.CreateStore("TestEagerAnalyticAliasFeedsLead", def.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta("TestEagerAnalyticAliasFeedsLead", "analytic", tempStore)
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	op := &AnalyticFuncsOp{FieldFuncs: []*ast.Call{latest, lead}}
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(2), "eligible": true, "b": false}}, fv, afv))
	require.Nil(t, op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(4), "eligible": false, "b": false}}, fv, afv))
	result := op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"ts": int64(5), "eligible": false, "b": true}}, fv, afv)
	ready := result.([]xsql.Row)
	require.Len(t, ready, 2)
	for _, resolved := range ready {
		value, ok := resolved.Value("$$a_lead_1", "")
		require.True(t, ok)
		require.Equal(t, int64(2), value)
	}
}
