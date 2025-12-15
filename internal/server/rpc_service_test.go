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

//go:build !core || (rpc && service)

package server

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/model"
	"github.com/lf-edge/ekuiper/v2/internal/service"
)

func TestRPCService(t *testing.T) {
	serviceManager, _ = service.InitManager()
	s := new(Server)
	reply := new(string)

	invalidID := "invalid/service"
	validID := "testSchema"
	// Create
	arg := &model.RPCArgDesc{
		Name: invalidID,
		Json: `{"name":"invalid/service","file":"file:///tmp/test.zip"}`,
	}
	err := s.CreateService(arg, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")

	schemaJson, _ := json.Marshal(&service.ServiceCreationRequest{
		Name: validID,
		File: "file:///tmp/test.zip",
	})
	arg = &model.RPCArgDesc{
		Name: validID,
		Json: string(schemaJson),
	}
	// Missing file, but validation passes
	err = s.CreateService(arg, reply)
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "invalid characters")

	// Desc
	err = s.DescService(invalidID, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")

	// DescFunc
	err = s.DescServiceFunc(invalidID, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")

	// Drop
	err = s.DropService(invalidID, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")
}
