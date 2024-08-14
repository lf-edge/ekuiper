// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package js

import (
	"fmt"
	"math"

	"github.com/dop251/goja"
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

// JSFunc is stateful
// Each instance has its own vm
type JSFunc struct {
	vm     *goja.Runtime
	jsfunc goja.Callable
	isAgg  bool
	// state, use this to avoid creating new array each time
	args []goja.Value
}

func NewJSFunc(symbolName string) (*JSFunc, error) {
	s, err := GetManager().GetScript(symbolName)
	if err != nil {
		return nil, fmt.Errorf("failed to get script for %s: %v", symbolName, err)
	}
	vm := goja.New()
	// Get the text from the symbol table
	_, _ = vm.RunString(s.Script)
	// Should not happen, already verify when install
	//if err != nil {
	//	return nil, fmt.Errorf("failed to interpret script: %v", err)
	//}
	exec, _ := goja.AssertFunction(vm.Get(symbolName))
	// Should not happen, already verify when install
	//if !ok {
	//	return nil, fmt.Errorf("cannot find function \"%s\" in script", symbolName)
	//}
	return &JSFunc{
		vm:     vm,
		jsfunc: exec,
		isAgg:  s.IsAgg,
	}, nil
}

func (f *JSFunc) Validate(_ []interface{}) error {
	return nil
}

func (f *JSFunc) Exec(ctx api.FunctionContext, args []any) (interface{}, bool) {
	ctx.GetLogger().Debugf("running js func with args %+v", args)
	if len(args) != len(f.args) {
		f.args = make([]goja.Value, len(args))
	}
	for i, arg := range args {
		f.args[i] = f.vm.ToValue(arg)
	}
	val, err := f.jsfunc(goja.Undefined(), f.args...)
	if err != nil {
		ctx.GetLogger().Errorf("failed to execute script: %v", err)
		return err, false
	} else {
		result := val.Export()
		switch t := result.(type) {
		case float64:
			if math.IsNaN(t) {
				return fmt.Errorf("result is NaN"), false
			}
			if math.IsInf(t, 0) {
				return fmt.Errorf("result is Inf"), false
			}
		}
		return result, true
	}
}

func (f *JSFunc) IsAggregate() bool {
	return f.isAgg
}

func (f *JSFunc) Close() error {
	return nil
}
