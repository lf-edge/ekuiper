package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"sync"
)

type registry struct {
	sync.RWMutex
	plugins   map[string]*PluginInfo
	functions map[string]string
}

// Set prerequisite: the pluginInfo must have been validated that the names are valid
func (r *registry) Set(name string, pi *PluginInfo) {
	r.Lock()
	defer r.Unlock()
	r.plugins[name] = pi
	for _, s := range pi.Functions {
		r.functions[s] = name
	}
}

func (r *registry) Get(name string) (*PluginInfo, bool) {
	r.RLock()
	defer r.RUnlock()
	// fib --> finonacci
	result, ok := r.plugins[name]
	fmt.Println("[plugin][wasm][registry.go][Get] name:", name)
	fmt.Println("[plugin][wasm][registry.go][Get] result:", result)
	return result, ok
}

func (r *registry) GetSymbol(pt plugin.PluginType, symbolName string) (string, bool) {
	switch pt {
	case plugin.FUNCTION:
		fmt.Println("[plugin][wasm][registry.go][GetSymbol] symbolName：", symbolName)
		s, ok := r.functions[symbolName]
		//s := symbolName
		fmt.Println("[plugin][wasm][registry.go][GetSymbol] s：", s)
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
	for _, s := range pi.Functions {
		delete(r.functions, s)
	}
}
