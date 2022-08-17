package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
)

var funcInsMap = &sync.Map{}

func (m *Manager) Function(name string) (api.Function, error) {
	fmt.Println("[plugin][wasm][factory.go] start")
	//ins, ok := funcInsMap.Load(name)
	//if ok {
	//	return ins.(api.Function), nil
	//}
	meta, ok := m.GetPluginMeta(plugin.FUNCTION, name)
	if !ok {
		return nil, nil
	}
	f, err := runtime.NewWasmFunc(name, meta)
	if err != nil {
		conf.Log.Errorf("Error creating function %v", err)
		return nil, err
	}
	funcInsMap.Store(name, f)
	return f, nil
}
