// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package wasm_test

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
	"testing"
	"time"
)

// EDIT HERE: Define the plugins that you want to test.
var testingPlugin = &wasm.PluginInfo{
	PluginMeta: runtime.PluginMeta{
		Name:       "fibonacci",
		Version:    "v1",
		WasmEngine: "wasmedge",
	},
	Functions: []string{"fib"},
}

var FuncData = [][]interface{}{
	{25.0}, {12.0}, // float
}

var (
	m       *wasm.Manager
	ctx     api.StreamContext
	cancels sync.Map
)

func TestExec(t *testing.T) {
	var err error
	m, err = wasm.MockManager(map[string]*wasm.PluginInfo{testingPlugin.Name: testingPlugin})
	if err != nil {
		panic(err)
	}
	c := context.WithValue(context.Background(), context.LoggerKey, conf.Log)
	ctx = c.WithMeta("rule1", "op1", &state.MemoryStore{}).WithInstance(1)
	ctrl := &runtime.Control{}
	f, err := m.Function(testingPlugin.Functions[0])
	if err != nil {
		fmt.Println("[wasm_test_server.go] err:", err)
		return
	}
	newctx, cancel := ctx.WithCancel()
	fc := context.NewDefaultFuncContext(newctx, 1)
	if _, ok := cancels.LoadOrStore(ctrl.PluginType+ctrl.SymbolName, cancel); ok {
		fmt.Println("[wasm_test_server.go] source symbol  already exists")
		return
	}
	for {
		for _, m := range FuncData {
			r, ok := f.Exec(m, fc)
			if !ok {
				fmt.Print("cannot exec func\n")
				continue
			}
			fmt.Println(r)
			//select {
			//case <-ctx.Done():
			//	ctx.GetLogger().Info("stop sink")
			//	return
			//default:
		}
		time.Sleep(1 * time.Second)
	}
}
