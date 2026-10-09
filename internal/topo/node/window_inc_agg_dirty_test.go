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

package node

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// GenerateAllFunctionState must only regenerate groups touched since the last
// generation, so per-row PutState stays O(touched groups). Untouched groups
// keep their last snapshot, which is still part of the persisted window.
func TestIncAggWindowDirtySnapshot(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	w := newIncAggWindow(ctx, time.Now())
	aggFields := []*ast.Field{
		{
			Name: "inc_agg_col_1",
			Expr: &ast.Call{
				Name:     "inc_count",
				FuncType: ast.FuncTypeScalar,
				Args:     []ast.Expr{&ast.Wildcard{Token: ast.ASTERISK}},
				FuncId:   1,
			},
		},
	}
	row := func(a int64) *xsql.Tuple {
		return &xsql.Tuple{Message: map[string]any{"a": a}}
	}
	incAggCal(ctx, "dim_a", row(1), w, aggFields)
	incAggCal(ctx, "dim_b", row(1), w, aggFields)
	w.GenerateAllFunctionState()
	require.Empty(t, w.dirtyDimensions)
	snapA := w.DimensionsIncAggRange["dim_a"].FunctionState
	snapB := w.DimensionsIncAggRange["dim_b"].FunctionState
	require.NotNil(t, snapA)
	require.NotNil(t, snapB)

	// Touch only dim_a: dim_b's snapshot must be reused as is.
	incAggCal(ctx, "dim_a", row(2), w, aggFields)
	require.Len(t, w.dirtyDimensions, 1)
	w.GenerateAllFunctionState()
	require.Empty(t, w.dirtyDimensions)
	require.Equal(t, reflect.ValueOf(snapB).Pointer(),
		reflect.ValueOf(w.DimensionsIncAggRange["dim_b"].FunctionState).Pointer(),
		"untouched group snapshot must be reused, not regenerated")
	require.NotEqual(t, reflect.ValueOf(snapA).Pointer(),
		reflect.ValueOf(w.DimensionsIncAggRange["dim_a"].FunctionState).Pointer(),
		"touched group snapshot must be refreshed")

	// Running aggregates stay correct.
	require.Equal(t, int64(2), w.DimensionsIncAggRange["dim_a"].Fields["inc_agg_col_1"])
	require.Equal(t, int64(1), w.DimensionsIncAggRange["dim_b"].Fields["inc_agg_col_1"])
}

// Cloned windows carry live state but no snapshot yet, so they must be fully
// regenerated on the next generation (e.g. sliding windows moved to EmitList).
func TestIncAggWindowCloneMarksDirty(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	w := newIncAggWindow(ctx, time.Now())
	aggFields := []*ast.Field{
		{
			Name: "inc_agg_col_1",
			Expr: &ast.Call{
				Name:     "inc_count",
				FuncType: ast.FuncTypeScalar,
				Args:     []ast.Expr{&ast.Wildcard{Token: ast.ASTERISK}},
				FuncId:   1,
			},
		},
	}
	incAggCal(ctx, "dim_a", &xsql.Tuple{Message: map[string]any{"a": int64(1)}}, w, aggFields)
	w.GenerateAllFunctionState()
	require.NotNil(t, w.DimensionsIncAggRange["dim_a"].FunctionState)

	c := w.Clone(ctx)
	require.Len(t, c.dirtyDimensions, 1)
	c.GenerateAllFunctionState()
	require.NotNil(t, c.DimensionsIncAggRange["dim_a"].FunctionState)
}
