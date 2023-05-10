// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"reflect"
	"strconv"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestAnalyticFuncs(t *testing.T) {
	var tests = []struct {
		funcs  []*ast.Call
		data   []interface{}
		result []map[string]interface{}
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
			result: []map[string]interface{}{{
				"$$a_lag_0": nil,
				"$$a_lag_1": nil,
			}, {
				"$$a_lag_0": "a1", "$$a_lag_1": "b1",
			}, {
				"$$a_lag_0": "a1", "$$a_lag_1": "b2",
			}, {
				"$$a_lag_0": "a1", "$$a_lag_1": interface{}(nil),
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
			result: []map[string]interface{}{
				{
					"$$a_changed_col_0": "a1", "$$a_had_changed_0": false, "$$a_lag_1": nil,
				}, {
					"$$a_changed_col_0": nil, "$$a_had_changed_0": true, "$$a_lag_1": "b1",
				}, {
					"$$a_changed_col_0": nil, "$$a_had_changed_0": false, "$$a_lag_1": nil,
				}, {
					"$$a_changed_col_0": nil, "$$a_had_changed_0": true, "$$a_lag_1": nil,
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestChangedFuncs_Apply1")

	for i, tt := range tests {
		tempStore, _ := state.CreateStore("mockRule"+strconv.Itoa(i), api.AtMostOnce)
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("mockRule"+strconv.Itoa(i), "project", tempStore)

		pp := &AnalyticFuncsOp{Funcs: tt.funcs}
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
		r := make([]map[string]interface{}, 0, len(tt.data))
		for _, d := range tt.data {
			opResult := pp.Apply(ctx, d, fv, afv)
			r = append(r, opResult.(*xsql.Tuple).CalCols)
		}

		if !reflect.DeepEqual(tt.result, r) {
			t.Errorf("%d.\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, r)
		}
	}
}
