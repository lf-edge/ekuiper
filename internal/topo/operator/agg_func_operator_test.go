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

package operator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestAggFuncOperator(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "select * from demo where a > avg(a) group by countwindow(2)",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
			},
			result: map[string]interface{}{"a": int64(6), "agg_ref_0": int64(6)},
		},
		{
			sql: "select * from demo where a > avg(a) group by countwindow(2)",
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{
						Message: xsql.Message{"a": int64(1)},
					},
					&xsql.Tuple{
						Message: xsql.Message{"a": int64(1)},
					},
				},
			},
			result: []map[string]interface{}{
				{
					"a":         int64(1),
					"agg_ref_0": int64(1),
				},
			},
		},
	}
	contextLogger := conf.Log.WithField("rule", "TestFilerPlan_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
			require.NoError(t, err)
			fv, afv := xsql.NewFunctionValuersForOp(ctx)
			pp := &AggFuncOp{AggFields: rewriteAggFunctionInWhere(stmt)}
			result := pp.Apply(ctx, tt.data, fv, afv)
			switch r := result.(type) {
			case *xsql.Tuple:
				require.Equal(t, tt.result, r.ToMap())
			case xsql.Collection:
				d := r.ToAggMaps()
				require.Equal(t, tt.result, d)
			}
		})
	}
}

func rewriteAggFunctionInWhere(stmt *ast.SelectStatement) []*ast.Field {
	aggFuncsFieldInWhere := make([]*ast.Field, 0)
	var index int
	ast.WalkFunc(stmt.Condition, func(node ast.Node) bool {
		switch aggFunc := node.(type) {
		case *ast.Call:
			if aggFunc.FuncType == ast.FuncTypeAgg {
				newAggFunc := &ast.Call{
					Name:     aggFunc.Name,
					FuncType: aggFunc.FuncType,
					Args:     aggFunc.Args,
					FuncId:   aggFunc.FuncId,
				}
				name := fmt.Sprintf("agg_ref_%v", index)
				newField := &ast.Field{
					Name: name,
					Expr: newAggFunc,
				}
				aggFuncsFieldInWhere = append(aggFuncsFieldInWhere, newField)
				newFieldRef := &ast.FieldRef{
					StreamName: ast.DefaultStream,
					Name:       name,
				}
				rewriteIntoBypass(newFieldRef, aggFunc)
			}
		}
		return true
	})
	return aggFuncsFieldInWhere
}

func rewriteIntoBypass(newFieldRef *ast.FieldRef, f *ast.Call) {
	f.FuncType = ast.FuncTypeScalar
	f.Args = []ast.Expr{newFieldRef}
	f.Name = "bypass"
}
