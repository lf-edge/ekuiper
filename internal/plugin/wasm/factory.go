// Copyright erfenjiao, 630166475@qq.com.
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

package wasm

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/pkg/api"
)

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
	return f, nil
}

func (m *Manager) HasFunctionSet(funcName string) bool {
	_, ok := m.reg.GetSymbol(plugin.FUNCTION, funcName)
	return ok
}

func (m *Manager) GetFunctionPlugin(funcName string) (plugin.EXTENSION_TYPE, string, string) {
	return plugin.NONE_EXTENSION, "", ""
}

func (m *Manager) ConvName(funcName string) (string, bool) {
	_, ok := m.GetPluginMeta(plugin.FUNCTION, funcName)
	return funcName, ok
}
