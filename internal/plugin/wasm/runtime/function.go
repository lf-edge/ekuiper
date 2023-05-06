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

package runtime

import (
	"fmt"
	"log"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/second-state/WasmEdge-go/wasmedge"
)

type WasmFunc struct {
	symbolName string
	reg        *PluginMeta
	isAgg      int
}

func NewWasmFunc(symbolName string, reg *PluginMeta) (*WasmFunc, error) {
	// Setup channel and route the data
	conf.Log.Infof("Start running  wasm function meta %+v", reg)

	return &WasmFunc{
		symbolName: symbolName,
		reg:        reg,
	}, nil
}

func (f *WasmFunc) Validate(args []interface{}) error {
	if len(args) == 0 {
		fmt.Println("[plugin][wasm][runtime][Validate] args is null")
	}
	var err error
	return err
}

func (f *WasmFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	res, err := f.ExecWasmFunc(args)
	if err != nil {
		return err, false
	}
	return res, true
}

func (f *WasmFunc) IsAggregate() bool {
	if f.isAgg > 0 {
		return f.isAgg > 1
	}
	return false
}

func toWasmEdgeValueSlideBindgen(vm *wasmedge.VM, modname *string, vals ...interface{}) ([]interface{}, error) {
	rvals := []interface{}{}

	for _, val := range vals {
		switch t := val.(type) {
		case wasmedge.FuncRef:
			rvals = append(rvals, val)
		case wasmedge.ExternRef:
			rvals = append(rvals, val)
		case wasmedge.V128:
			rvals = append(rvals, val)
		case int32:
			rvals = append(rvals, val)
		case uint32:
			rvals = append(rvals, val)
		case int64:
			rvals = append(rvals, val)
		case uint64:
			rvals = append(rvals, val)
		case int:
			rvals = append(rvals, val)
		case uint:
			rvals = append(rvals, val)
		case float32:
			rvals = append(rvals, val)
		case float64:
			rvals = append(rvals, val)
		case string:
			// Call malloc function
			sval := []byte(val.(string))
			mallocsize := uint32(len(sval))
			var rets []interface{}
			var err error = nil
			if modname == nil {
				rets, err = vm.Execute("malloc", mallocsize+1)
			} else {
				rets, err = vm.ExecuteRegistered(*modname, "malloc", mallocsize)
			}
			if err != nil {
				return nil, fmt.Errorf("toWasmEdgeValueSlideBindgen(): malloc failed with error %v", err)
			}
			if len(rets) <= 0 {
				return nil, fmt.Errorf("toWasmEdgeValueSlideBindgen(): malloc function signature unexpected")
			}
			argaddr := rets[0]
			rvals = append(rvals, argaddr)
			// Set bytes
			var mod *wasmedge.Module = nil
			var mem *wasmedge.Memory = nil
			if modname == nil {
				mod = vm.GetActiveModule()
			} else {
				store := vm.GetStore()
				mod = store.FindModule(*modname)
			}
			if mod != nil {
				memnames := mod.ListMemory()
				if len(memnames) <= 0 {
					return nil, fmt.Errorf("toWasmEdgeValueSlideBindgen(): memory instance not found")
				}
				mem = mod.FindMemory(memnames[0])
				mem.SetData(sval, uint(rets[0].(int32)), uint(mallocsize))
				mem.SetData([]byte{0}, uint(rets[0].(int32)+int32(mallocsize)), 1)
			}
		case []byte:
			// Call malloc function
			mallocsize := uint32(len(val.([]byte)))
			var rets []interface{}
			var err error = nil
			if modname == nil {
				rets, err = vm.Execute("malloc", mallocsize)
			} else {
				rets, err = vm.ExecuteRegistered(*modname, "malloc", mallocsize)
			}
			if err != nil {
				return nil, fmt.Errorf("toWasmEdgeValueSlideBindgen(): malloc failed")
			}
			if len(rets) <= 0 {
				return nil, fmt.Errorf("toWasmEdgeValueSlideBindgen(): malloc function signature unexpected")
			}
			argaddr := rets[0]
			argsize := mallocsize
			rvals = append(rvals, argaddr, argsize)
			// Set bytes
			var mod *wasmedge.Module = nil
			var mem *wasmedge.Memory = nil
			if modname == nil {
				mod = vm.GetActiveModule()
			} else {
				store := vm.GetStore()
				mod = store.FindModule(*modname)
			}
			if mod != nil {
				memnames := mod.ListMemory()
				if len(memnames) <= 0 {
					return nil, fmt.Errorf("toWasmEdgeValueSlideBindgen(): memory instance not found")
				}
				mem = mod.FindMemory(memnames[0])
				mem.SetData(val.([]byte), uint(rets[0].(int32)), uint(mallocsize))
			}
		default:
			return nil, fmt.Errorf("wrong argument of toWasmEdgeValueSlideBindgen(): %T not supported", t)
		}
	}
	return rvals, nil
}

func (f *WasmFunc) ExecWasmFunc(args []interface{}) ([]interface{}, error) {
	funcname := f.symbolName

	WasmFile := f.reg.WasmFile
	fmt.Println("[wasm][ExecWasmFunc] WasmFile: ", WasmFile)
	conf1 := wasmedge.NewConfigure(wasmedge.WASI)
	store := wasmedge.NewStore()
	vm := wasmedge.NewVMWithConfigAndStore(conf1, store)
	wasi := vm.GetImportModule(wasmedge.WASI)
	// step 1: Load WASM file
	err := vm.LoadWasmFile(WasmFile)
	if err != nil {
		fmt.Print("[wasm][ExecWasmFunc] Load WASM from file FAILED: ")
		return nil, err
	}
	// step 2: Validate the WASM module
	err = vm.Validate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Validate FAILED: ")
		return nil, err
	}
	// step 3: Instantiate the WASM moudle
	err = vm.Instantiate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Instantiate FAILED: ")
		return nil, err
	}
	// step 4: Execute WASM functions.Parameters(1)
	Args, err := toWasmEdgeValueSlideBindgen(vm, nil, args...)
	if err != nil {
		return nil, err
	}

	var res []interface{}
	res, err = vm.Execute(funcname, Args...)
	if err != nil {
		log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		return nil, err
	} else {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
		fmt.Println(res[0])
	}
	exitcode := wasi.WasiGetExitCode()
	if exitcode != 0 {
		fmt.Println("Go: Running wasm failed, exit code:", exitcode)
	}
	vm.Release()
	return res, nil
}
