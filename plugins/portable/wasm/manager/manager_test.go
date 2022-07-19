package manager

import (
	"fmt"
	"log"

	//"github.com/lf-edge/ekuiper/plugins/portable/wasm/types"
	"testing"
)

func TestGetConfig(t *testing.T) {
	/*
		w := new(conf)
		w.GetConfig2()
	*/
	fmt.Println("[test][wasm][test-TestGetConfig] start")
	w := new(WasmPluginConfig)
	w = w.GetConfig()
	//if w.GetConfig() != false {
	//	fmt.Println("[wasm][test]GetConfig() successful!!")
	//}
	//fmt.Printf("w.getconf(): %v\n", w.getConf())
}

func TestWasmPluginConfig_AddWasmPlugin(t *testing.T) {
	fmt.Println("[test][wasm][manager-AddWasmPlugin] start")
	fmt.Println("[test][wasm][manager-AddWasmPlugin] GetConfig")
	w := new(WasmPluginConfig)
	w = w.GetConfig()
	//fmt.Println("[test][wasm][manager-AddWasmPlugin] Add PluginName:", w.PluginName)
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
}

func TestWasmPluginConfig_GetWasmPluginConfigByName(t *testing.T) {
	fmt.Println("[test][wasm][manager-GetWasmPluginConfigByName] start")
	w := new(WasmPluginConfig)
	w = w.GetConfig()
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	fmt.Println("[test][wasm][manager-GetWasmPluginConfigByName] ", w.GetWasmPluginConfigByName(w.PluginName))
}
