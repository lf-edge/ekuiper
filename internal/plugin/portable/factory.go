// Copyright 2021 EMQ Technologies Co., Ltd.
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

func (m *Manager) Sink(name string) (api.Sink, error) {
	meta, ok := m.GetPluginMeta(plugin.SINK, name)
	if !ok {
		return nil, nil
	}
	return runtime.NewPortableSink(name, meta), nil
}

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

// Clean up function map
func (m *Manager) Clean() {
	funcInsMap.Range(func(_, ins interface{}) bool {
		f := ins.(*runtime.PortableFunc)
		_ = f.Close()
		return true
	})
}
