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
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type FuncOp struct {
	CallExpr *ast.Call
	Name     string
}

func (p *FuncOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	ctx.GetLogger().Debugf("FuncOp receive: %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.Valuer:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
		result := ve.Eval(p.CallExpr)
		if e, ok := result.(error); ok {
			return e
		}
		switch val := input.(type) {
		case xsql.Row:
			val.Set(p.Name, result)
			return val
		default:
			return fmt.Errorf("unknow type")
		}
	default:
		return fmt.Errorf("run func error: invalid input %[1]T(%[1]v)", input)
	}
}
