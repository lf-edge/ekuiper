// Copyright 2022 EMQ Technologies Co., Ltd.
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

//go:build portable && rpc && core && !plugin
// +build portable,rpc,core,!plugin

package server

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
)

func (t *Server) doRegister(pt plugin.PluginType, p plugin.Plugin) error {
	if pt == plugin.WASM {
		return wasmManager.Register(p)
	} else {
		return fmt.Errorf("wasm plugin support is disabled")
	}
}

func (t *Server) doDelete(pt plugin.PluginType, name string, stopRun bool) error {
	if pt == plugin.WASM {
		return wasmManager.Delete(name)
	} else {
		return fmt.Errorf("wasm plugin support is disabled")
	}
}

func (t *Server) doDesc(pt plugin.PluginType, name string) (interface{}, error) {
	if pt == plugin.WASM {
		r, ok := wasmManager.GetPluginInfo(name)
		if !ok {
			return nil, fmt.Errorf("not found")
		}
		return r, nil
	} else {
		return nil, fmt.Errorf("wasm plugin support is disabled")
	}
}
