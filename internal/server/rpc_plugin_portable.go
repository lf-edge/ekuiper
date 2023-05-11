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
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/internal/plugin"
)

func (t *Server) doRegister(pt plugin.PluginType, p plugin.Plugin) error {
	if pt == plugin.PORTABLE {
		return portableManager.Register(p)
	} else {
		return fmt.Errorf("native plugin support is disabled")
	}
}

func (t *Server) doDelete(pt plugin.PluginType, name string, stopRun bool) error {
	if pt == plugin.PORTABLE {
		return portableManager.Delete(name)
	} else {
		return fmt.Errorf("native plugin support is disabled")
	}
}

func (t *Server) doDesc(pt plugin.PluginType, name string) (interface{}, error) {
	if pt == plugin.PORTABLE {
		r, ok := portableManager.GetPluginInfo(name)
		if !ok {
			return nil, fmt.Errorf("not found")
		}
		return r, nil
	} else {
		return nil, fmt.Errorf("native plugin support is disabled")
	}
}

func (t *Server) doShow(pt plugin.PluginType) (string, error) {
	if pt == plugin.PORTABLE {
		l := portableManager.List()
		jb, err := json.Marshal(l)
		if err != nil {
			return "", err
		}
		return string(jb), nil
	} else {
		return "", fmt.Errorf("native plugin support is disabled")
	}
}
