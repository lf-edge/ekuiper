// Copyright 2024 EMQ Technologies Co., Ltd.
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

package conf

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEnv(t *testing.T) {
	IsTesting = true
	em := &EnvManager{}
	require.NoError(t, os.Setenv("CONNECTION__MQTT__ENVDEMO__SERVER", "tcp://127.0.0.1:1883"))
	require.NoError(t, os.Setenv("CONNECTION__MQTT__ENVDEMO__TOPIC", "mock"))
	require.NoError(t, os.Setenv("CONNECTION__MQTT__ENVDEMO__PORT", "1883"))
	em.Setup()
	em.SetupConnectionProps()
	g, err := GetCfgFromKVStorage("connections", "mqtt", "envdemo")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"server": "tcp://127.0.0.1:1883",
		"topic":  "mock",
		"port":   int64(1883),
	}, g["connections.mqtt.envdemo"])
	require.Nil(t, em.connectionProps)
}
