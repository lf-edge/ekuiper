package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/second-state/WasmEdge-go/wasmedge"
	"log"
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
	//TODO implement me
	//panic("implement me")
	fmt.Println("[plugin][wasm][runtime][function.go][Validate] start: ")
	jsonArg, err := encode("Validate", args)
	fmt.Println("[plugin][wasm][runtime][function.go][Validate] (string)jsonArg: ", string(jsonArg))
	if err != nil {
		return err
	}
	return err
}

func (f *WasmFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	fmt.Println("[plugin][wasm][runtime][function.go][Exec] start: ")
	ctx.GetLogger().Debugf("running wasm func with args %+v", args)
	ctxRaw, err := encodeCtx(ctx)
	if err != nil {
		return err, false
	}

	res := f.ExecWasmFunc(args)

	jsonArg, err := encode("Exec", append(res, ctxRaw))
	//fmt.Println("[internal][plugin][wasm][runtime][function.go] jsonArg(string):", string(jsonArg))
	if err != nil {
		return err, false
	}
	fr := &FuncReply{}
	err = json.Unmarshal(jsonArg, fr)
	if err != nil {
		return err, false
	}
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
	//TODO implement me
	//panic("implement me")
	if f.isAgg > 0 {
		return f.isAgg > 1
	}
	return false
}

func (f *WasmFunc) Close() {
	return
}

func encode(funcName string, arg interface{}) ([]byte, error) {
	c := FuncData{
		Func: funcName,
		Arg:  arg,
	}
	return json.Marshal(c)
}

func encodeCtx(ctx api.FunctionContext) (string, error) {
	m := FuncMeta{
		Meta: Meta{
			RuleId:     ctx.GetRuleId(),
			OpId:       ctx.GetOpId(),
			InstanceId: ctx.GetInstanceId(),
		},
		FuncId: ctx.GetFuncId(),
	}
	bs, err := json.Marshal(m)
	return string(bs), err
}

func (f *WasmFunc) ExecWasmFunc(args []interface{}) []interface{} {
	funcname := f.symbolName
	fmt.Println("[internal][plugin][wasm][runtime][function.go] funcname: ", funcname)
	WasmFile := f.reg.WasmFile
	conf1 := wasmedge.NewConfigure(wasmedge.WASI)
	store := wasmedge.NewStore()
	vm := wasmedge.NewVMWithConfigAndStore(conf1, store)
	wasi := vm.GetImportModule(wasmedge.WASI)
	//step 1: Load WASM file
	err := vm.LoadWasmFile(WasmFile)
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Load WASM from file FAILED: ")
		fmt.Errorf(err.Error())
	}
	//step 2: Validate the WASM module
	err = vm.Validate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Validate FAILED: ")
		fmt.Errorf(err.Error())
	}
	//step 3: Instantiate the WASM moudle
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
			fmt.Println("Type tranform Failed!!")
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
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
			fmt.Println(res[0].(int32))
		}
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	case 1:
		res, err = vm.Execute(funcname, uint32(Args[0]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
			fmt.Println(res[0].(int32))
		}
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	case 2:
		res, err = vm.Execute(funcname, uint32(Args[0]), uint32(Args[1]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
			fmt.Println(res[0].(int32))
		}
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	case 3:
		res, err = vm.Execute(funcname, uint32(Args[0]), uint32(Args[1]), uint32(Args[2]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get res: ")
			fmt.Println(res[0].(int32))
		}
		exitcode := wasi.WasiGetExitCode()
		if exitcode != 0 {
			fmt.Println("Go: Running wasm failed, exit code:", exitcode)
		}
		vm.Release()
	}
	return res
}
