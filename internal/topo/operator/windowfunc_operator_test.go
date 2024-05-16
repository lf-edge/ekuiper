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

package operator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestWindowFuncApplyCollection(t *testing.T) {
	data1 := &xsql.WindowTuples{
		Content: []xsql.Row{
			&xsql.Tuple{
				Message: map[string]interface{}{
					"a": 1,
					"b": 1,
				},
			},
			&xsql.Tuple{
				Message: map[string]interface{}{
					"a": 1,
					"b": 2,
				},
			},
			&xsql.Tuple{
				Message: map[string]interface{}{
					"a": 2,
					"b": 1,
				},
			},
			&xsql.Tuple{
				Message: map[string]interface{}{
					"a": 2,
					"b": 2,
				},
			},
		},
	}
	data2 := &xsql.WindowTuples{
		Content: []xsql.Row{
			&xsql.Tuple{
				Message: map[string]interface{}{
					"a": 1,
					"b": 2,
				},
			},
			&xsql.Tuple{
				Message: map[string]interface{}{
					"a": 1,
					"b": 1,
				},
			},
		},
	}
	testcases := []struct {
		data   *xsql.WindowTuples
		op     *WindowFuncOperator
		expect []map[string]interface{}
	}{
		{
			data: data1,
			op: &WindowFuncOperator{
				WindowFuncField: ast.Field{
					Name: "row_number",
					Expr: &ast.Call{
						Name: "row_number",
						Partition: &ast.PartitionExpr{
							Exprs: []ast.Expr{
								&ast.FieldRef{StreamName: "demo", Name: "a"},
							},
						},
						SortFields: []ast.SortField{
							{
								Name:       "b",
								StreamName: "",
								Uname:      "b",
								Ascending:  true,
								FieldExpr: &ast.FieldRef{
									StreamName: "demo",
									Name:       "b",
								},
							},
						},
					},
				},
			},
			expect: []map[string]interface{}{
				{
					"a":          1,
					"b":          1,
					"row_number": 1,
				},
				{
					"a":          1,
					"b":          2,
					"row_number": 2,
				},
				{
					"a":          2,
					"b":          1,
					"row_number": 1,
				},
				{
					"a":          2,
					"b":          2,
					"row_number": 2,
				},
			},
		},
		{
			data: data2,
			op: &WindowFuncOperator{
				WindowFuncField: ast.Field{
					Name: "row_number",
					Expr: &ast.Call{
						Name: "row_number",
						SortFields: []ast.SortField{
							{
								Name:       "b",
								StreamName: "",
								Uname:      "b",
								Ascending:  true,
								FieldExpr: &ast.FieldRef{
									StreamName: "demo",
									Name:       "b",
								},
							},
						},
					},
				},
			},
			expect: []map[string]interface{}{
				{
					"a":          1,
					"b":          1,
					"row_number": 1,
				},
				{
					"a":          1,
					"b":          2,
					"row_number": 2,
				},
			},
		},
	}
	for _, tc := range testcases {
		contextLogger := conf.Log.WithField("rule", "TestWindowFuncApplyCollection")
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		output := tc.op.Apply(ctx, tc.data, fv, afv).(xsql.Collection)
		require.Equal(t, tc.expect, output.ToMaps())
	}
}
