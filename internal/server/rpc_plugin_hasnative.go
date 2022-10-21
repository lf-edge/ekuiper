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

//go:build !core || (rpc && plugin)

package server

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"strings"
)

func (t *Server) RegisterPlugin(arg *model.PluginDesc, reply *string) error {
	p, err := getPluginByJson(arg, plugin.FUNCTION)
	if err != nil {
		return fmt.Errorf("Register plugin functions error: %s", err)
	}
	if len(p.GetSymbols()) == 0 {
		return fmt.Errorf("Register plugin functions error: Missing function list.")
	}
	err = nativeManager.RegisterFuncs(p.GetName(), p.GetSymbols())
	if err != nil {
		return fmt.Errorf("Create plugin error: %s", err)
	} else {
		*reply = fmt.Sprintf("Plugin %s is created.", p.GetName())
	}
	return nil
}

func (t *Server) ShowUdfs(_ int, reply *string) error {
	l := nativeManager.ListSymbols()
	if len(l) == 0 {
		l = append(l, "No udf is found.")
	}
	*reply = strings.Join(l, "\n")
	return nil
}

func (t *Server) DescUdf(arg string, reply *string) error {
	m, ok := nativeManager.GetPluginBySymbol(plugin.FUNCTION, arg)
	if !ok {
		return fmt.Errorf("Describe udf error: not found")
	} else {
		j := map[string]string{
			"name":   arg,
			"plugin": m,
		}
		r, err := marshalDesc(j)
		if err != nil {
			return fmt.Errorf("Describe udf error: %v", err)
		}
		*reply = r
	}
	return nil
}
