// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package xsql

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func validateFuncs(funcName string, args []ast.Expr) error {
	f, err := function.Function(funcName)
	if f != nil {
		var targs []interface{}
		for _, arg := range args {
			targs = append(targs, arg)
		}
		if mf, ok := f.(MultiFunc); ok {
			return mf.ValidateWithName(args, funcName)
		} else {
			return f.Validate(targs)
		}
	} else {
		if err != nil {
			return err
		} else {
			return fmt.Errorf("function %s not found", funcName)
		}
	}
}

func ExecFunc(funcName string, f api.Function, args []interface{}, fctx api.FunctionContext) (interface{}, bool) {
	if mf, ok := f.(MultiFunc); ok {
		return mf.ExecWithName(args, fctx, funcName)
	} else {
		return f.Exec(args, fctx)
	}
}

// MultiFunc hack for builtin functions that works for multiple functions
type MultiFunc interface {
	ValidateWithName(args []ast.Expr, name string) error
	ExecWithName(args []interface{}, ctx api.FunctionContext, name string) (interface{}, bool)
}
