package wasm

//func ExecWasmFile(args []interface{}) {
//	fmt.Println("[sdk][go][example][fib][fib-Exec] start: ")
//	fmt.Println("[sdk][go][example][fib][fib-Exec] args: ", args)
//	fmt.Println("[sdk][go][example][fib][fib-Exec] len(args): ", len(args))
//	f.funcName = "fib"
//	//result := args[0]
//	var result interface{}
//	//VmEngine := w.WasmPluginConfig.VmConfig.EngineName
//	//fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] VmEngine: ", VmEngine)
//	conf := wasmedge.NewConfigure()
//	store := wasmedge.NewStore()
//
//	vm := wasmedge.NewVMWithConfigAndStore(conf, store)
//	//step 1: Load WASM file
//	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 1: Load WASM file: ")
//	err := vm.LoadWasmFile("fibonacci.wasm")
//	if err != nil {
//		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Load WASM from file FAILED: ")
//		fmt.Errorf(err.Error())
//	}
//	//step 2: Validate the WASM module
//	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 2: Validate the WASM module")
//	err = vm.Validate()
//	if err != nil {
//		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Validate FAILED: ")
//		fmt.Errorf(err.Error())
//	}
//	//step 3: Instantiate the WASM moudle
//	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 3: Instantiate the WASM moudle")
//	err = vm.Instantiate()
//	if err != nil {
//		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Instantiate FAILED: ")
//		fmt.Errorf(err.Error())
//	}
//	Len := len(args)
//	switch Len {
//	case 0:
//		//w.WasmEngine.vm.Execute(Function)
//		fmt.Println("[wasm][manager-ExecuteFunction] step 4: Execute WASM functions.Parameter(0)")
//		res, err := vm.Execute(f.funcName)
//		if err != nil {
//			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
//		} else {
//			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
//			fmt.Println(res[0].(int32))
//		}
//		result = res
//		vm.Release()
//	case 1:
//		fmt.Println("[wasm][manager-ExecuteFunction] step 4: Execute WASM functions.Parameters(1): ", args[0])
//		//fmt.Println("[wasm][manager-ExecuteFunction] function: ", w.WasmPluginConfig.Function)
//		value := args[0].(float64)
//		fmt.Println("The value is ", value)
//		fmt.Println("The value(int32) is ", int32(value))
//		res, err := vm.Execute(f.funcName, uint32(value))
//		if err != nil {
//			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
//		} else {
//			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
//			fmt.Println(res[0].(int32))
//		}
//		result = res
//		vm.Release()
//	case 2:
//		fmt.Println("[wasm][manager-ExecuteFunction] step 4: Execute WASM functions.Parameters(2): ", args[0], args[1])
//		res, err := vm.Execute(f.funcName, args[0], args[1])
//		if err != nil {
//			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
//		} else {
//			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
//			fmt.Println(res[0].(int32))
//		}
//		result = res
//		vm.Release()
//	}
//	vm.Release()
//}
