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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"sync"
)

type registry struct {
	sync.RWMutex
	plugins map[string]*PluginInfo
	// mapping from symbol to plugin. Deduced from plugin set.
	sources   map[string]string
	sinks     map[string]string
	functions map[string]string
}

// Set prerequisite: the pluginInfo must have been validated that the names are valid
func (r *registry) Set(name string, pi *PluginInfo) {
	r.Lock()
	defer r.Unlock()
	r.plugins[name] = pi
	for _, s := range pi.Sources {
		r.sources[s] = name
	}
	for _, s := range pi.Sinks {
		r.sinks[s] = name
	}
	for _, s := range pi.Functions {
		r.functions[s] = name
	}
}

func (r *registry) Get(name string) (*PluginInfo, bool) {
	r.RLock()
	defer r.RUnlock()
	result, ok := r.plugins[name]
	fmt.Println("[plugin][portable][registry.go][Get] result, ok:", result, ok)
	return result, ok
}

func (r *registry) GetSymbol(pt plugin.PluginType, symbolName string) (string, bool) {
	switch pt {
	case plugin.SOURCE:
		s, ok := r.sources[symbolName]
		return s, ok
	case plugin.SINK:
		s, ok := r.sinks[symbolName]
		return s, ok
	case plugin.FUNCTION:
		s, ok := r.functions[symbolName]
		return s, ok
	default:
		return "", false
	}
}

func (r *registry) List() []*PluginInfo {
	r.RLock()
	defer r.RUnlock()
	// return empty slice instead of nil to help json marshal
	result := make([]*PluginInfo, 0, len(r.plugins))
	for _, v := range r.plugins {
		result = append(result, v)
	}
	return result
}

func (r *registry) Delete(name string) {
	r.Lock()
	defer r.Unlock()
	pi, ok := r.plugins[name]
	if !ok {
		return
	}
	delete(r.plugins, name)
	for _, s := range pi.Sources {
		delete(r.sources, s)
	}
	for _, s := range pi.Sinks {
		delete(r.sinks, s)
	}
	for _, s := range pi.Functions {
		delete(r.functions, s)
	}
}
