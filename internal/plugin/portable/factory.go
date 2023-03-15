// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package portable

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
)

func (m *Manager) Source(name string) (api.Source, error) {
	meta, ok := m.GetPluginMeta(plugin.SOURCE, name)
	if !ok {
		return nil, nil
	}
	return runtime.NewPortableSource(name, meta), nil
}

func (m *Manager) GetSourcePlugin(name string) (plugin.EXTENSION_TYPE, string, string) {
	pluginName, _ := m.reg.GetSymbol(plugin.SOURCE, name)
	var installScript = ""
	m.plgInstallDb.Get(pluginName, &installScript)
	return plugin.PORTABLE_EXTENSION, pluginName, installScript
}

func (m *Manager) LookupSource(_ string) (api.LookupSource, error) {
	// TODO add support
	return nil, nil
}

func (m *Manager) Sink(name string) (api.Sink, error) {
	meta, ok := m.GetPluginMeta(plugin.SINK, name)
	if !ok {
		return nil, nil
	}
	return runtime.NewPortableSink(name, meta), nil
}

func (m *Manager) GetSinkPlugin(name string) (plugin.EXTENSION_TYPE, string, string) {
	pluginName, _ := m.reg.GetSymbol(plugin.SINK, name)
	var installScript = ""
	m.plgInstallDb.Get(pluginName, &installScript)
	return plugin.PORTABLE_EXTENSION, pluginName, installScript
}

// The function instance are kept forever even after deletion
// The instance is actually a wrapper of the nng channel which is dependant from the plugin instance
// Even updated plugin instance can reuse the channel if the function name is not changed
// It is not used to check if the function is bound, use ConvName which checks the meta
var funcInsMap = &sync.Map{}

func (m *Manager) Function(name string) (api.Function, error) {
	ins, ok := funcInsMap.Load(name)
	if ok {
		return ins.(api.Function), nil
	}
	meta, ok := m.GetPluginMeta(plugin.FUNCTION, name)
	if !ok {
		return nil, nil
	}
	f, err := runtime.NewPortableFunc(name, meta)
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

func (m *Manager) GetFunctionPlugin(funcName string) (plugin.EXTENSION_TYPE, string, string) {
	pluginName, _ := m.reg.GetSymbol(plugin.FUNCTION, funcName)
	var installScript = ""
	m.plgInstallDb.Get(pluginName, &installScript)
	return plugin.PORTABLE_EXTENSION, pluginName, installScript
}

func (m *Manager) ConvName(funcName string) (string, bool) {
	_, ok := m.GetPluginMeta(plugin.FUNCTION, funcName)
	return funcName, ok
}
