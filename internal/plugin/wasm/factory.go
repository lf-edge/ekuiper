package wasm

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
)

var funcInsMap = &sync.Map{}

func (m *Manager) Function(name string) (api.Function, error) {
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

func (m *Manager) HasFunctionSet(funcName string) bool {
	_, ok := m.reg.GetSymbol(plugin.FUNCTION, funcName)
	return ok
}

func (m *Manager) ConvName(funcName string) (string, bool) {
	_, ok := m.GetPluginMeta(plugin.FUNCTION, funcName)
	return funcName, ok
}
