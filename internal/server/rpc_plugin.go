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

//go:build (rpc || !core) && (plugin || portable || !core)

package server

import (
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/internal/hack"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/plugin"
)

func (t *Server) CreatePlugin(arg *model.PluginDesc, reply *string) error {
	pt := plugin.PluginType(arg.Type)
	p, err := getPluginByJson(arg, pt)
	if err != nil {
		return fmt.Errorf("Create plugin error: %s", err)
	}
	if p.GetFile() == "" {
		return fmt.Errorf("Create plugin error: Missing plugin file url.")
	}
	// define according to the build tag
	err = t.doRegister(pt, p)
	if err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("Create plugin error: %s", err)
	} else {
		*reply = fmt.Sprintf("Plugin %s is created.", p.GetName())
	}
	return nil
}

func (t *Server) DropPlugin(arg *model.PluginDesc, reply *string) error {
	pt := plugin.PluginType(arg.Type)
	p, err := getPluginByJson(arg, pt)
	if err != nil {
		return fmt.Errorf("Drop plugin error: %s", err)
	}
	err = t.doDelete(pt, p.GetName(), arg.Stop)
	if err != nil {
		return fmt.Errorf("Drop plugin error: %s", err)
	} else {
		if pt == plugin.PORTABLE {
			*reply = fmt.Sprintf("Plugin %s is dropped .", p.GetName())
		} else {
			if arg.Stop {
				*reply = fmt.Sprintf("Plugin %s is dropped and Kuiper will be stopped.", p.GetName())
			} else {
				*reply = fmt.Sprintf("Plugin %s is dropped and Kuiper must restart for the change to take effect.", p.GetName())
			}
		}
	}

	return nil
}

func (t *Server) DescPlugin(arg *model.PluginDesc, reply *string) error {
	pt := plugin.PluginType(arg.Type)
	p, err := getPluginByJson(arg, pt)
	if err != nil {
		return fmt.Errorf("Describe plugin error: %s", err)
	}
	m, err := t.doDesc(pt, p.GetName())
	if err != nil {
		return fmt.Errorf("Describe plugin error: %s", err)
	} else {
		r, err := marshalDesc(m)
		if err != nil {
			return fmt.Errorf("Describe plugin error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) ShowPlugins(arg int, reply *string) error {
	pt := plugin.PluginType(arg)
	l, err := t.doShow(pt)
	if err != nil {
		return fmt.Errorf("Show plugin error: %s", err)
	}
	*reply = l
	return nil
}

func getPluginByJson(arg *model.PluginDesc, pt plugin.PluginType) (plugin.Plugin, error) {
	p := plugin.NewPluginByType(pt)
	if arg.Json != "" {
		if err := json.Unmarshal(hack.StringToBytes(arg.Json), p); err != nil {
			return nil, fmt.Errorf("Parse plugin %s error : %s.", arg.Json, err)
		}
	}
	p.SetName(arg.Name)
	return p, nil
}
