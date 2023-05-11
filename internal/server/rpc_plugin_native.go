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

//go:build plugin && rpc && core && !portable
// +build plugin,rpc,core,!portable

package server

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/internal/plugin"
)

func (t *Server) doRegister(pt plugin.PluginType, p plugin.Plugin) error {
	if pt == plugin.PORTABLE {
		return fmt.Errorf("portable plugin support is disabled")
	} else {
		return nativeManager.Register(pt, p)
	}
}

func (t *Server) doDelete(pt plugin.PluginType, name string, stopRun bool) error {
	if pt == plugin.PORTABLE {
		return fmt.Errorf("portable plugin support is disabled")
	} else {
		return nativeManager.Delete(pt, name, stopRun)
	}
}

func (t *Server) doDesc(pt plugin.PluginType, name string) (interface{}, error) {
	if pt == plugin.PORTABLE {
		return nil, fmt.Errorf("portable plugin support is disabled")
	} else {
		r, ok := nativeManager.GetPluginInfo(pt, name)
		if !ok {
			return nil, fmt.Errorf("not found")
		}
		return r, nil
	}
}

func (t *Server) doShow(pt plugin.PluginType) (string, error) {
	var result string
	if pt == plugin.PORTABLE {
		return "", fmt.Errorf("portable plugin support is disabled")
	} else {
		l := nativeManager.List(pt)
		if len(l) == 0 {
			result = "No plugin is found."
		} else {
			result = strings.Join(l, "\n")
		}
		return result, nil
	}
}
