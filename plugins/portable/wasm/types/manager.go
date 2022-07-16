package types

//var manager *WasmManager

type WasmManager interface {
	GetConfig()
	AddWasmPlugin()
	UpdateWasmPlugin()
	DeleteWasmPlugin(pluginName string) error
}
