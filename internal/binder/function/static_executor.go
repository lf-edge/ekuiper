// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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

package function

import (
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type funcExecutor struct{}

func (f *funcExecutor) ValidateWithName(args []ast.Expr, name string) error {
	fs, ok := builtins[name]
	if !ok {
		return errors.New("unknown name")
	}

	eargs := make([]ast.Expr, len(args))
	copy(eargs, args)
	// TODO pass in ctx
	err := fs.val(nil, eargs)
	if err != nil {
		return err
	}
	return nil
}

func (f *funcExecutor) Validate(_ []interface{}) error {
	return fmt.Errorf("unknow name")
}

func (f *funcExecutor) Exec(ctx api.FunctionContext, args []any) (interface{}, bool) {
	return fmt.Errorf("unknow name"), false
}

func (f *funcExecutor) ExecWithName(args []interface{}, ctx api.FunctionContext, name string) (interface{}, bool) {
	fs, ok := builtins[name]
	if !ok {
		return fmt.Errorf("unknow name"), false
	}
	if fs.check != nil {
		r, skipExec := fs.check(args)
		if skipExec {
			return r, true
		}
	}
	return fs.exec(ctx, args)
}

func (f *funcExecutor) IsAggregate() bool {
	return false
}

func (f *funcExecutor) GetFuncType(name string) ast.FuncType {
	fs, ok := builtins[name]
	if !ok {
		return ast.FuncTypeUnknown
	}
	return fs.fType
}

var staticFuncExecutor = &funcExecutor{}
