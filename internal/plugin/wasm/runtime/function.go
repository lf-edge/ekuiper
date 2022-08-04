package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/second-state/WasmEdge-go/wasmedge"
	"log"
)

type WasmFunc struct {
	symbolName string
	reg        *PluginMeta
	dataCh     DataReqChannel
	isAgg      int
}

func NewPortableFunc(symbolName string, reg *PluginMeta) (*WasmFunc, error) {
	// Setup channel and route the data
	conf.Log.Infof("Start running  wasm function meta %+v", reg)
	pm := GetPluginInsManager()
	ins, err := pm.getOrStartProcess(reg, WasmConf)
	if err != nil {
		return nil, err
	}
	conf.Log.Infof("Plugin started successfully")

	// Create function channel
	dataCh, err := CreateFunctionChannel(symbolName)
	if err != nil {
		return nil, err
	}

	// Start symbol
	c := &Control{
		SymbolName: symbolName,
		PluginType: TYPE_FUNC,
	}
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, conf.Log)
	err = ins.StartSymbol(ctx, c)
	if err != nil {
		fmt.Println("[plugin][wasm][runtime][function.go] StartSymbol err: ", err)
		return nil, err
	}

	err = dataCh.Handshake()
	if err != nil {
		return nil, fmt.Errorf("function %s handshake error: %v", reg.Name, err)
	}

	return &WasmFunc{
		symbolName: reg.Name,
		reg:        reg,
		dataCh:     dataCh,
	}, nil
}

func (f *WasmFunc) Validate(args []interface{}) error {
	//TODO implement me
	//panic("implement me")
	fmt.Println("[plugin][wasm][runtime][function.go][Validate] start: ")
	jsonArg, err := encode("Validate", args)
	fmt.Println("[plugin][wasm][runtime][function.go][Validate] (string)jsonArg: ", jsonArg)
	if err != nil {
		return err
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		return err
	}
	fr := &FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		return err
	}
	if fr.State {
		return nil
	} else {
		return fmt.Errorf("validate return state is false, got %+v", fr)
	}
}

func (f *WasmFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	//TODO implement me
	//panic("implement me")
	fmt.Println("[plugin][wasm][runtime][function.go][Exec] start: ")
	ctx.GetLogger().Debugf("running wasm func with args %+v", args)
	ctxRaw, err := encodeCtx(ctx)
	fmt.Println("[internal][plugin][wasm][runtime][function.go] ctxRaw: ", ctxRaw)
	//{"ruleId":"rule1","opId":"op1","instanceId":1,"funcId":1}
	if err != nil {
		return err, false
	}
	funcname := f.symbolName
	fmt.Println("[internal][plugin][wasm][runtime][function.go] funcname: ", funcname)
	WasmFile := f.reg.WasmFile
	fmt.Println("[internal][plugin][wasm][runtime][function.go] WasmFile: ", WasmFile)
	//--------------------------------------
	conf := wasmedge.NewConfigure()
	store := wasmedge.NewStore()
	vm := wasmedge.NewVMWithConfigAndStore(conf, store)
	//step 1: Load WASM file
	//fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 1: Load WASM file: ")
	err = vm.LoadWasmFile(WasmFile)
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Load WASM from file FAILED: ")
		fmt.Errorf(err.Error())
	}
	//step 2: Validate the WASM module
	//fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 2: Validate the WASM module")
	err = vm.Validate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Validate FAILED: ")
		fmt.Errorf(err.Error())
	}
	//step 3: Instantiate the WASM moudle
	//fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 3: Instantiate the WASM moudle")
	err = vm.Instantiate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Instantiate FAILED: ")
		fmt.Errorf(err.Error())
	}
	// step 4: Execute WASM functions.Parameters(1)
	//fmt.Println("[wasm][manager-ExecuteFunction] step 4: Execute WASM functions.Parameters(1): ", args[0])
	//fmt.Println("[wasm][manager-ExecuteFunction] function: ", w.WasmPluginConfig.Function)
	value := args[0].(int)
	//fmt.Println("The value is ", value)
	fmt.Println("The value(uint32) is ", uint32(value))
	res, err := vm.Execute(funcname, uint32(value))
	if err != nil {
		log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
	} else {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
		fmt.Println(res[0].(int32))
	}
	fr := &FuncReply{}
	//fr.Result = res
	vm.Release()
	//---------------------------------------
	//jsonArg, err := encode("Exec", append(args, ctxRaw))
	//if err != nil {
	//	return err, false
	//}
	//fmt.Println("[internal][plugin][wasm][runtime][function.go] jsonArg(string):", string(jsonArg))
	//{"func":"Exec","arg":["twelve","{\"ruleId\":\"rule1\",\"opId\":\"op1\",\"instanceId\":1,\"funcId\":1}"]}
	jsonArg, err := encode("Exec", append(res, ctxRaw))
	fmt.Println("[internal][plugin][wasm][runtime][function.go] jsonArg(string):", string(jsonArg))
	res2, err := f.dataCh.Req(jsonArg)
	if err != nil {
		return err, false
	}

	err = json.Unmarshal(res2, fr)
	if err != nil {
		return err, false
	}
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
	jsonArg, err := encode("IsAggregate", nil)
	if err != nil {
		conf.Log.Error(err)
		return false
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		conf.Log.Error(err)
		return false
	}
	fr := &FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		conf.Log.Error(err)
		return false
	}
	if fr.State {
		r, ok := fr.Result.(bool)
		if !ok {
			conf.Log.Errorf("IsAggregate result is not bool, got %s", string(res))
			return false
		} else {
			if r {
				f.isAgg = 2
			} else {
				f.isAgg = 1
			}
			return r
		}
	} else {
		conf.Log.Errorf("IsAggregate return state is false, got %+v", fr)
		return false
	}
}

func (f *WasmFunc) Close() error {
	return f.dataCh.Close()
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

func ExecWasmFunc(funcName string, args interface{}) {

}
