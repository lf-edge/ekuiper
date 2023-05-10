// Copyright erfenjiao, 630166475@qq.com.
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

	"github.com/second-state/WasmEdge-go/wasmedge"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
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
	res := f.ExecWasmFunc(args)

	fr := &FuncReply{}
	fr.Result = res
	fr.State = true
	if !fr.State {
		if fr.Result != nil {
			return fmt.Errorf("%s", fr.Result), false
		} else {
			return nil, false
		}
	}
	return fr.Result, fr.State
}

func (f *WasmFunc) IsAggregate() bool {
	if f.isAgg > 0 {
		return f.isAgg > 1
	}
	return false
}

func (f *WasmFunc) ExecWasmFunc(args []interface{}) []interface{} {
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
		fmt.Errorf(err.Error())
	}
	// step 2: Validate the WASM module
	err = vm.Validate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Validate FAILED: ")
		fmt.Errorf(err.Error())
	}
	// step 3: Instantiate the WASM moudle
	err = vm.Instantiate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Instantiate FAILED: ")
		fmt.Errorf(err.Error())
	}
	// step 4: Execute WASM functions.Parameters(1)
	var Args []float64
	for _, num := range args {
		x, ok := (num).(float64)
		if !ok {
			fmt.Println("Type tranform not to float64!!")
		}
		Args = append(Args, x)
	}

	Len := len(args)
	var res []interface{}
	switch Len {
	case 0:
		res, err = vm.Execute(funcname)
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		}
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
		fmt.Println(res[0].(int32))
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	case 1:
		res, err = vm.Execute(funcname, uint32(Args[0]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		}
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
		fmt.Println(res[0].(int32))
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	case 2:
		res, err = vm.Execute(funcname, uint32(Args[0]), uint32(Args[1]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		}
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
		fmt.Println(res[0].(int32))
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	case 3:
		res, err = vm.Execute(funcname, uint32(Args[0]), uint32(Args[1]), uint32(Args[2]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		}
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
		fmt.Println(res[0].(int32))
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	}
	return res
}
