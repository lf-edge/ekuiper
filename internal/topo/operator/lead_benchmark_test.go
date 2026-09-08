// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

// Each partition has one pending row, isolating global snapshot work from
// per-partition matching. One benchmark operation processes a whole batch.
func BenchmarkLeadSparsePartitions(b *testing.B) {
	for _, size := range []int{128, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			store, _ := state.CreateStore(b.Name(), def.AtMostOnce)
			base := context.WithValue(context.Background(), context.LoggerKey, conf.Log)
			call := &ast.Call{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.FieldRef{Name: "v"}}, Partition: &ast.PartitionExpr{Exprs: []ast.Expr{&ast.FieldRef{Name: "v"}}}}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				ctx := base.WithMeta(b.Name(), "analytic", store)
				fv, afv := xsql.NewFunctionValuersForOp(ctx)
				op := &AnalyticFuncsOp{Funcs: []*ast.Call{call}}
				for i := 0; i < size; i++ {
					op.Apply(ctx, &xsql.Tuple{Message: xsql.Message{"v": i}}, fv, afv)
				}
			}
		})
	}
}
