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

package function

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

type funcExe func(ctx api.FunctionContext, args []interface{}) (interface{}, bool)
type funcVal func(ctx api.FunctionContext, args []ast.Expr) error

type builtinFunc struct {
	fType ast.FuncType
	exec  funcExe
	val   funcVal
}

var builtins map[string]builtinFunc

func init() {
	builtins = make(map[string]builtinFunc)
	registerAggFunc()
	registerMathFunc()
	registerStrFunc()
	registerMiscFunc()
	registerAnalyticFunc()
	registerColsFunc()
}

//var funcWithAsteriskSupportMap = map[string]string{
//	"collect": "",
//	"count":   "",
//}

var analyticFuncs = map[string]struct{}{
	"lag":         {},
	"changed_col": {},
	"had_changed": {},
}

const AnalyticPrefix = "$$a"

func IsAnalyticFunc(name string) bool {
	_, ok := analyticFuncs[name]
	return ok
}

type funcExecutor struct{}

func (f *funcExecutor) ValidateWithName(args []ast.Expr, name string) error {
	fs, ok := builtins[name]
	if !ok {
		return fmt.Errorf("validate function %s error: unknown name", name)
	}

	var eargs []ast.Expr
	for _, arg := range args {
		if t, ok := arg.(ast.Expr); ok {
			eargs = append(eargs, t)
		} else {
			// should never happen
			return fmt.Errorf("receive invalid arg %v", arg)
		}
	}
	// TODO pass in ctx
	return fs.val(nil, eargs)
}

func (f *funcExecutor) Validate(_ []interface{}) error {
	return fmt.Errorf("unknow name")
}

func (f *funcExecutor) Exec(_ []interface{}, _ api.FunctionContext) (interface{}, bool) {
	return fmt.Errorf("unknow name"), false
}

func (f *funcExecutor) ExecWithName(args []interface{}, ctx api.FunctionContext, name string) (interface{}, bool) {
	fs, ok := builtins[name]
	if !ok {
		return fmt.Errorf("unknow name"), false
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

type Manager struct{}

// Function the name is converted to lowercase if needed during parsing
func (m *Manager) Function(name string) (api.Function, error) {
	_, ok := builtins[name]
	if !ok {
		return nil, nil
	}
	return staticFuncExecutor, nil
}

func (m *Manager) HasFunctionSet(name string) bool {
	return name == "internal"
}

func (m *Manager) ConvName(n string) (string, bool) {
	name := strings.ToLower(n)
	_, ok := builtins[name]
	return name, ok
}

var m = &Manager{}

func GetManager() *Manager {
	return m
}
