// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build script

package operator

import (
	"fmt"
	"github.com/dop251/goja"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type ScriptOp struct {
	vm     *goja.Runtime
	jsfunc goja.Callable
	isAgg  bool
}

func NewScriptOp(script string, isAgg bool) (*ScriptOp, error) {
	vm := goja.New()
	_, err := vm.RunString(script)
	if err != nil {
		return nil, fmt.Errorf("failed to interprete script: %v", err)
	}
	exec, ok := goja.AssertFunction(vm.Get("exec"))
	if !ok {
		return nil, fmt.Errorf("cannot find function \"exec\" in script")
	}
	n := &ScriptOp{
		vm:     vm,
		jsfunc: exec,
		isAgg:  isAgg,
	}
	return n, nil
}

func (p *ScriptOp) Apply(ctx api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	ctx.GetLogger().Debugf("ScriptOp receive: %s", data)
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		val, err := p.jsfunc(goja.Undefined(), p.vm.ToValue(input.ToMap()), p.vm.ToValue(input.Metadata))
		if err != nil {
			return fmt.Errorf("failed to execute script: %v", err)
		} else {
			nm, ok := val.Export().(map[string]interface{})
			if !ok {
				return fmt.Errorf("script exec result is not a map: %v", val.Export())
			} else {
				return &xsql.Tuple{Message: nm, Metadata: input.Metadata, Emitter: input.Emitter, Timestamp: input.Timestamp}
			}
		}
	case xsql.Collection:
		val, err := p.jsfunc(goja.Undefined(), p.vm.ToValue(input.ToMaps()))
		if err != nil {
			return fmt.Errorf("failed to execute script: %v", err)
		} else {
			switch nm := val.Export().(type) {
			case map[string]interface{}:
				if !p.isAgg {
					return fmt.Errorf("script node is not aggregate but exec result is aggregated: %v", val.Export())

				}
				return &xsql.Tuple{Message: nm}
			case []map[string]interface{}:
				if p.isAgg {
					return fmt.Errorf("script node is aggregate but exec result is not aggreagated: %v", val.Export())
				}
				w := &xsql.WindowTuples{}
				for _, v := range nm {
					if v != nil {
						w.Content = append(w.Content, &xsql.Tuple{Message: v})
					}
				}
				return w
			default:
				return fmt.Errorf("script exec result is not a map or array of map: %v", val.Export())
			}
		}
	default:
		return fmt.Errorf("run script op invalid input allow tuple only but got %[1]T(%[1]v)", input)
	}
}
