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
	"github.com/lf-edge/ekuiper/internal/plugin"
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

var (
	builtins            map[string]builtinFunc
	builtinStatfulFuncs map[string]func() api.Function
)

func init() {
	builtins = make(map[string]builtinFunc)
	builtinStatfulFuncs = make(map[string]func() api.Function)
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
	"latest":      {},
}

const AnalyticPrefix = "$$a"

func IsAnalyticFunc(name string) bool {
	_, ok := analyticFuncs[name]
	return ok
}

type Manager struct{}

// Function the name is converted to lowercase if needed during parsing
func (m *Manager) Function(name string) (api.Function, error) {
	_, ok := builtins[name]
	if ok {
		return staticFuncExecutor, nil
	}
	ff, ok := builtinStatfulFuncs[name]
	if ok {
		return ff(), nil
	}
	return nil, nil
}

func (m *Manager) HasFunctionSet(name string) bool {
	return name == "internal"
}

func (m *Manager) FunctionPluginInfo(funcName string) (plugin.EXTENSION_TYPE, string, string) {
	_, ok := builtins[funcName]
	if !ok {
		return plugin.NONE_EXTENSION, "", ""
	} else {
		return plugin.INTERNAL, "", ""
	}
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
