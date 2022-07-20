package manager

import (
	"github.com/second-state/WasmEdge-go/wasmedge"
	"sync"
)

type VmConfig struct {
	EngineName string `yaml:"engineName"` //执行引擎
	Path       string `yaml:"path"`       //文件路径
}

type WasmPluginConfig struct {
	PluginName string   `yaml:"pluginName"`
	VmConfig   VmConfig `yaml:"vmConfig"`
	//path        string   `yaml:"path"`
	InstanceNum int    `yaml:"instanceNum"`
	Function    string `yaml:"function"`
}

type WasmEngine struct {
	vm *wasmedge.VM //好像没什么用，vm地址无法索引到
}

type WasmManager struct {
	WasmPluginConfig         WasmPluginConfig // Config
	WasmEngine               WasmEngine
	WasmPluginMap            sync.Map
	WasmFunctionIntParameter []int
}
