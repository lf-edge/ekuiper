package manager

import (
	"fmt"
	"log"

	//"github.com/lf-edge/ekuiper/plugins/portable/wasm/types"
	"testing"
)

func TestGetConfig(t *testing.T) {
	w := new(WasmManager)
	fmt.Println("[test][wasm][test-TestGetConfig] start")
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	//if w.GetConfig() != false {
	//	fmt.Println("[wasm][test]GetConfig() successful!!")
	//}
	//fmt.Printf("w.getconf(): %v\n", w.getConf())
}

func TestWasmPluginConfig_AddWasmPlugin(t *testing.T) {
	fmt.Println("[test][wasm][manager-AddWasmPlugin] AddWasmPlugin Start")
	//fmt.Println("[test][wasm][manager-AddWasmPlugin] GetConfig")
	w := new(WasmManager)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	//fmt.Println("[test][wasm][manager-AddWasmPlugin] Add PluginName:", w.PluginName)
	if w.AddWasmPlugin(w.WasmPluginConfig.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	} else {
		fmt.Println("[test][wasm][manager-AddWasmPlugin] Add successful!!!")
	}
	fmt.Println("[test][wasm][manager-AddWasmPlugin] Execute")
	//var args []int
	//args = append(args, 25)
	w.WasmFunctionIntParameter = append(w.WasmFunctionIntParameter, 25)
	//w.NewWasmPlugin()
	w.ExecuteFunction()
}

func TestWasmPluginConfig_GetWasmPluginConfig(t *testing.T) {
	fmt.Println("[test][wasm][manager-GetWasmPluginConfigByName] start")
	w := new(WasmManager)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.WasmPluginConfig.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	fmt.Println("[test][wasm][manager-GetWasmPluginConfigByName] Config: ", w.GetWasmPluginConfig("etc1"))
}

func TestWasmPluginConfig_DeleteWasmPluginConfig(t *testing.T) {
	fmt.Println("[test][wasm][manager-DeleteWasmPluginConfig] delete start")
	w := new(WasmManager)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.WasmPluginConfig.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	if w.DeleteWasmPluginConfigByName("etc1") == false {
		log.Fatalln("[test][wasm][manager-DeleteWasmPluginConfig] delete failed!!")
	} else {
		fmt.Println("[test][wasm][manager-DeleteWasmPluginConfig] delete successful!!!")
	}
}

func TestDeleteWasmPluginConfigNyName(t *testing.T) {
	fmt.Println("[test][wasm][manager-DeleteWasmPluginConfig] delete start")
	w := new(WasmManager)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.WasmPluginConfig.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	DeletePluginName := "etc1"
	if w.DeleteWasmPluginConfigByName(DeletePluginName) == false {
		log.Fatalln("[test][wasm][manager-DeleteWasmPluginConfig] delete failed!!")
	}
}

func TestCheckRepeat(t *testing.T) {
	w := new(WasmManager)
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc1.yaml")
	if w.AddWasmPlugin(w.WasmPluginConfig.PluginName) == false {
		log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
	}
	w = w.GetConfig("/home/erfenjiao/ekuiper/plugins/portable/wasm/etc/etc2.yaml")
	if w.AddWasmPlugin(w.WasmPluginConfig.PluginName) == false {
		fmt.Println("[test][wasm][manager-AddWasmPlugin] search repeated Plugin, delete")
		//log.Fatalln("[test][wasm][manager-AddWasmPlugin] Add FAILED!!!")
		if w.DeleteWasmPluginConfigByName(w.WasmPluginConfig.PluginName) == false {
			log.Fatalln("[test][wasm][delete] failed!!")
		}
		fmt.Println("[test][wasm][delete] delete successful!!")
	}
}
