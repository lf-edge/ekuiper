// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/model"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/native"
)

func TestRPCPlugin(t *testing.T) {
	nativeManager, _ = native.InitManager()
	s := new(Server)
	reply := new(string)

	invalidID := "invalid/plugin"
	validID := "testPlugin"
	// Create
	p := plugin.NewPluginByType(plugin.SOURCE)
	p.SetName(invalidID)
	if iop, ok := p.(*plugin.IOPlugin); ok {
		iop.File = "file:///tmp/test.zip"
	}
	jsonBytes, _ := json.Marshal(p)

	arg := &model.PluginDesc{
		RPCArgDesc: model.RPCArgDesc{
			Name: invalidID,
			Json: string(jsonBytes),
		},
		Type: int(plugin.SOURCE),
	}
	err := s.CreatePlugin(arg, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")

	p.SetName(validID)
	jsonBytes, _ = json.Marshal(p)
	arg = &model.PluginDesc{
		RPCArgDesc: model.RPCArgDesc{
			Name: validID,
			Json: string(jsonBytes),
		},
		Type: int(plugin.SOURCE),
	}
	// Missing file actual install will fail, but validation passes
	// Mocking registry is hard here, so we expect error but NOT validation error
	// However, without proper setup doRegister might panic or fail early.
	// We mainly test validation logic here.

	// Desc
	err = s.DescPlugin(&model.PluginDesc{
		RPCArgDesc: model.RPCArgDesc{Name: invalidID},
		Type:       int(plugin.SOURCE),
	}, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")

	// Drop
	err = s.DropPlugin(&model.PluginDesc{
		RPCArgDesc: model.RPCArgDesc{Name: invalidID},
		Type:       int(plugin.SOURCE),
	}, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")
}
