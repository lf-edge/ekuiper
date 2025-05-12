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
