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
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	//if w.GetConfig() != false {
	//	fmt.Println("[wasm][test]GetConfig() successful!!")
	//}
	//fmt.Printf("w.getconf(): %v\n", w.getConf())
}

func TestWasmPluginConfig_AddWasmPlugin(t *testing.T) {
	fmt.Println("[test][wasm][manager-AddWasmPlugin] start")
	fmt.Println("[test][wasm][manager-AddWasmPlugin] GetConfig")
	w := new(WasmPluginConfig)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	//fmt.Println("[test][wasm][manager-AddWasmPlugin] Add PluginName:", w.PluginName)
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
}

func TestWasmPluginConfig_GetWasmPluginConfig(t *testing.T) {
	fmt.Println("[test][wasm][manager-GetWasmPluginConfigByName] start")
	w := new(WasmPluginConfig)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	fmt.Println("[test][wasm][manager-GetWasmPluginConfigByName] ", w.GetWasmPluginConfig())
}

func TestWasmPluginConfig_DeleteWasmPluginConfig(t *testing.T) {
	fmt.Println("[test][wasm][manager-DeleteWasmPluginConfig] delete start")
	w := new(WasmPluginConfig)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	if w.DeleteWasmPluginConfig() == false {
		log.Fatalln("[test][wasm][manager-DeleteWasmPluginConfig] delete failed!!")
	}
}

func TestCheckRepeat(t *testing.T) {
	w := new(WasmPluginConfig)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc2.yaml")
	if w.AddWasmPlugin(w.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}

}
