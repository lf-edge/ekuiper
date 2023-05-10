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
	"reflect"
	"sync"
	"testing"

	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
)

func TestConcurrent(t *testing.T) {
	r := &registry{
		RWMutex:   sync.RWMutex{},
		plugins:   make(map[string]*PluginInfo),
		sources:   make(map[string]string),
		sinks:     make(map[string]string),
		functions: make(map[string]string),
	}
	allPlugins := []*PluginInfo{
		{
			PluginMeta: runtime.PluginMeta{
				Name:       "mirror",
				Version:    "1.3.0",
				Language:   "go",
				Executable: "mirror",
			},
			Sources:   []string{"random"},
			Sinks:     []string{"file"},
			Functions: []string{"echo"},
		}, {
			PluginMeta: runtime.PluginMeta{
				Name:       "next",
				Version:    "1.3.0",
				Language:   "python",
				Executable: "next",
			},
			Sinks: []string{"udp", "follower"},
		}, {
			PluginMeta: runtime.PluginMeta{
				Name:       "dummy",
				Version:    "v0.2",
				Language:   "go",
				Executable: "dummy",
			},
			Sources:   []string{"new", "can"},
			Functions: []string{"abc"},
		},
	}
	expectedPlugins := map[string]*PluginInfo{
		"mirror": allPlugins[0],
		"next":   allPlugins[1], "dummy": allPlugins[2],
	}

	expectedSources := map[string]string{
		"can": "dummy", "new": "dummy", "random": "mirror",
	}
	expectedFunctions := map[string]string{
		"abc": "dummy", "echo": "mirror",
	}
	expectedSinks := map[string]string{
		"file": "mirror", "follower": "next", "udp": "next",
	}
	// set concurrently
	var wg sync.WaitGroup
	for n, pi := range expectedPlugins {
		wg.Add(1)
		go func(name string, pluginInfo *PluginInfo) {
			defer wg.Done()
			r.Set(name, pluginInfo)
		}(n, pi)
	}
	wg.Wait()

	if !reflect.DeepEqual(expectedPlugins, r.plugins) {
		t.Errorf("plugins mismatch: expected %v, got %v", expectedPlugins, r.plugins)
		return
	}
	result := r.List()
	if !reflect.DeepEqual(len(allPlugins), len(result)) {
		t.Errorf("list plugins count mismatch: expected %v, got %v", allPlugins, result)
		return
	}
outer:
	for _, res := range result {
		for _, p := range allPlugins {
			if reflect.DeepEqual(p, res) {
				continue outer
			}
		}
		t.Errorf("list plugins mismatch: expected %v, got %v", allPlugins, result)
		return
	}

	if !reflect.DeepEqual(expectedSources, r.sources) {
		t.Errorf("sources mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", expectedSources, r.sources)
		return
	}
	if !reflect.DeepEqual(expectedFunctions, r.functions) {
		t.Errorf("functions mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", expectedFunctions, r.functions)
		return
	}
	if !reflect.DeepEqual(expectedSinks, r.sinks) {
		t.Errorf("sinks mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", expectedSinks, r.functions)
		return
	}
	pn, ok := r.GetSymbol(plugin.SOURCE, "new")
	if !ok {
		t.Error("can't find symbol new")
		return
	}
	if pn != "dummy" {
		t.Errorf("GetSymbol wrong, expect dummy but got %s", pn)
	}

	// Delete concurrently
	for n := range expectedPlugins {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			r.Delete(name)
		}(n)
	}
	wg.Wait()
	result = r.List()
	if !reflect.DeepEqual(0, len(result)) {
		t.Errorf("list plugins count mismatch: expected no plugins, got %v", result)
		return
	}
}
