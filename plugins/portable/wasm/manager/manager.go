package manager

import (
	"encoding/json"
	"fmt"
	"github.com/second-state/WasmEdge-go/wasmedge"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

func (w *WasmManager) GetConfig(YamlFile string) *WasmManager {
	//conf := w.getConf()
	yamlFile, err := ioutil.ReadFile(YamlFile)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = yaml.UnmarshalStrict(yamlFile, w.WasmPluginConfig)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Print("[wasm][manager-GetConfig()] WasmPluginConfig:\t")
	fmt.Println(w)
	//fmt.Print("[wasm][manager-GetConfig] conf:\t")
	//fmt.Println(conf)

	//将对象，转换成json格式
	data, err := json.Marshal(w)
	if err != nil {
		log.Fatalln("[wasm][manager-GetConfig] err:\t", err.Error())
	}

	fmt.Println("[wasm][manager-GetConfig] data:\t", string(data))

	//fmt.Print("[wasm][manager-GetConfig]w = ")
	//fmt.Println(w)
	//w.WasmPluginMap[w.PluginName] = *w
	//w *WasmPluginConfig
	return w
}

func NewWasmPlugin(config WasmManager) bool {
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] NewWasmPlugin start")
	//ensure  engine
	VmEngine := config.WasmPluginConfig.VmConfig.EngineName
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] VmEngine: ", VmEngine)
	conf := wasmedge.NewConfigure()
	store := wasmedge.NewStore()

	vm := wasmedge.NewVMWithConfigAndStore(conf, store)
	//step 1: Load WASM file
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 1: Load WASM file: ", config.VmConfig.Path)
	err := vm.LoadWasmFile(config.WasmPluginConfig.VmConfig.Path)
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Load WASM from file FAILED: ")
		log.Fatalln(err.Error())
	}
	//step 2: Validate the WASM module
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 2: Validate the WASM module")
	err = vm.Validate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Validate FAILED: ")
		log.Fatalln(err.Error())
	}
	//step 3: Instantiate the WASM moudle
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 3: Instantiate the WASM moudle")
	err = vm.Instantiate()
	if err != nil {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Instantiate FAILED: ")
		log.Fatalln(err.Error())
	}
	//step 4: Execute WASM functions.Parameters: (funcname, args...)
	//
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] step 4: Execute WASM functions.Parameters: (funcname, args...)")
	function := config.WasmPluginConfig.Function
	res, err := vm.Execute(function, uint32(25))
	if err != nil {
		log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
	} else {
		fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
		fmt.Println(res[0].(int32))
	}

	config.WasmEngine.vm = vm

	//w.WasmPluginMap.LoadOrStore(w.PluginName, &w)
	//WasmPluginMap.LoadOrStore(config.PluginName, config)
	test, _ := config.WasmPluginMap.Load(config.WasmPluginConfig.PluginName)
	fmt.Println("[wasm][manager-AddWasmPlugin-NewWasmPlugin] map LoadOrStore ,map: ", test)

	return true
}

func (w *WasmManager) ExecuteFunction(Function string) {
	var args []int
	for i := 0; i < len(w.WasmFunctionIntParameter); i++ {
		args = append(args, w.WasmFunctionIntParameter[i])
	}
	len := len(args)
	switch len {
	case 0:
		//w.WasmEngine.vm.Execute(Function)
		res, err := w.WasmEngine.vm.Execute(Function, uint32(args[0]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
			fmt.Println(res[0].(int32))
		}
	case 1:
		//w.WasmEngine.vm.Execute(Function, args[0])
		res, err := w.WasmEngine.vm.Execute(Function, uint32(args[0]), uint32(args[1]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
			fmt.Println(res[0].(int32))
		}
	case 2:
		res, err := w.WasmEngine.vm.Execute(Function, uint32(args[0]), uint32(args[1]), uint32(args[2]))
		if err != nil {
			log.Fatalln("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Run function failed： ", err.Error())
		} else {
			fmt.Print("[wasm][manager-AddWasmPlugin-NewWasmPlugin] Get fibonacci[25]: ")
			fmt.Println(res[0].(int32))
		}
	}
}

// add

func (w *WasmManager) AddWasmPlugin(PluginName string) bool {
	//config := w.GetConfig()
	fmt.Print("[wasm][manager-AddWasmPlugin] w.PluginName:\t")
	fmt.Println(w.WasmPluginConfig.PluginName)
	if w.WasmPluginConfig.PluginName == "" {
		log.Fatalln("[wasm][manager-AddWasmPlugin] pluginName is empty")
	}

	// if exist

	if v, ok := w.WasmPluginMap.Load(w.WasmPluginConfig.PluginName); ok {
		if !ok {
			log.Fatalln("[wasm][manager-AddWasmPlugin] unexpected type in map")
		}
		fmt.Println("[wasm][manager-AddWasmPlugin] Plugin already exit, you also can delete this map v:", v)
		return false
	}

	//
	// add new wasm plugin
	if NewWasmPlugin(*w) == false {
		fmt.Println("[wasm][manager-AddWasmPlugin] NewWasmPlugin failed !!!")
	} else {
		fmt.Println("[wasm][manager-AddWasmPlugin] NewWasmPlugin successful")
	}
	return true
}

// search

func (w *WasmManager) GetWasmPluginConfig() WasmPluginConfig {
	if w.WasmPluginConfig.PluginName == "" {
		log.Fatalln("[error][wasm][manager-GetWasmPluginConfigByName] PluginName is nil")
	}
	if v, ok := w.WasmPluginMap.Load(w.WasmPluginConfig.PluginName); ok {
		//pw := new(WasmPluginConfig)
		pw, ok := v.(WasmPluginConfig)
		if !ok {
			log.Fatalln("[error][wasm][manager-GetWasmPluginConfigByName] unexpected object type in map, v: ", v)
		}
		fmt.Println("[wasm][manager-GetWasmPluginConfigByName] v: ", v)
		return pw
	}
	log.Fatalln("[error][wasm][manager-GetWasmPluginConfigByName] not found !!!")
	err := new(WasmPluginConfig)
	return *err
}

//// update
//
//func (w *WasmPluginConfig) UpdateWasmPluginConfig() bool {
//	fmt.Print("[wasm][manager-UpdateWasmPluginConfig] update start")
//
//}
//
//// delete
//
//func (w *WasmPluginConfig) DeleteWasmPluginConfig() bool {
//	v, ok := WasmPluginMap.Load(w.PluginName)
//	if !ok {
//		log.Fatalln("[error][wasm][manager-DeleteWasmPluginConfigByName] plugin not found, v: ", v)
//	}
//
//	WasmPluginMap.Delete(w.PluginName)
//	return true
//}
//
//// delete by name
//func (w *WasmPluginConfig) DeleteWasmPluginConfigByName(PluginConfigName string) bool {
//	v, ok := WasmPluginMap.Load(PluginConfigName)
//	if !ok {
//		log.Fatalln("[error][wasm][manager-DeleteWasmPluginConfigByName] plugin not found, v: ", v)
//	}
//	WasmPluginMap.Delete(PluginConfigName)
//	return true
//}

// abi
//

// yaml test
type conf struct {
	Host   string `yaml:"host"`
	User   string `yaml:"user"`
	Pwd    string `yaml:"pwd"`
	Dbname string `yaml:"dbname"`
}

func (c *conf) GetConfig2() {
	//var c conf
	//读取yaml配置文件
	conf := c.getConf()
	fmt.Println(conf)

	//将对象，转换成json格式
	data, err := json.Marshal(conf)

	if err != nil {
		fmt.Println("err:\t", err.Error())
		return
	}

	//最终以json格式，输出
	fmt.Println("data:\t", string(data))
}

//读取Yaml配置文件,
//并转换成conf对象
func (c *conf) getConf() *conf {
	//应该是 绝对地址
	yamlFile, err := ioutil.ReadFile("E:\\Program\\go2\\goPath\\src\\xingej-go\\xingej-go\\xingej-go666\\lib\\yaml\\conf.yaml")
	if err != nil {
		fmt.Println(err.Error())
	}

	err = yaml.Unmarshal(yamlFile, c)

	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("c:\t", c)

	return c
}
