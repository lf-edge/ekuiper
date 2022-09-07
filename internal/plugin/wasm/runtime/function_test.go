package runtime

import "testing"




func (f *WasmFunc) fTestNewWasmFunc(t *testing.T) {
	name := f.symbolName := "fibonacci"
	f.reg.WasmFile := "/home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci.wasm"

	ExecWasmFunc
}
