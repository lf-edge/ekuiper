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

package meta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

func init() {
	InitYamlConfigManager()
}

func createPaths() {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	dirs := []string{"sources", "sinks", "functions", "services", "services/schemas", "connections"}

	for _, v := range dirs {
		// Create dir if not exist
		realDir := filepath.Join(dataDir, v)
		if _, err := os.Stat(realDir); os.IsNotExist(err) {
			if err := os.MkdirAll(realDir, os.ModePerm); err != nil {
				panic(err)
			}
		}
	}

	files := []string{"connections/connection.yaml"}
	for _, v := range files {
		// Create dir if not exist
		realFile := filepath.Join(dataDir, v)
		if _, err := os.Stat(realFile); os.IsNotExist(err) {
			if _, err := os.Create(realFile); err != nil {
				panic(err)
			}
		}
	}
}

func TestYamlConfigMeta_Ops(t *testing.T) {
	createPaths()

	plgName := "mocksource"
	addData := `{"url":"127.0.0.1","method":"post","headers":{"Accept":"json"}}`
	// init new ConfigOperator, success
	err := AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err != nil {
		t.Error(err)
	}

	// Exist ConfigKey , fail
	err = AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err != nil {
		t.Error("should overwrite exist config key")
	}
}

func TestConfKeyErr(t *testing.T) {
	err := DelSourceConfKey("1", "2", "3")
	require.Error(t, err)
	ewc, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	err = DelSinkConfKey("1", "2", "3")
	require.Error(t, err)
	ewc, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	err = DelConnectionConfKey("1", "2", "3")
	require.Error(t, err)
	ewc, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	_, err = GetYamlConf("1", "2")
	require.Error(t, err)
	ewc, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	err = AddSourceConfKey("1", "2", "3", nil)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	err = AddSinkConfKey("1", "2", "3", nil)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
}

func TestValidateConf(t *testing.T) {
	c, err := json.Marshal(map[string]interface{}{"path": "/123"})
	require.NoError(t, err)
	require.NoError(t, validateConf("websocket", map[string]interface{}{"path": "/123"}, true))
	require.NoError(t, validateConf("websocket", map[string]interface{}{"path": "/123"}, false))
	require.NoError(t, AddSinkConfKey("websocket", "k1", "en-us", c))
	require.NoError(t, AddSourceConfKey("websocket", "k2", "en-us", c))
}

func TestReplaceConfigurations(t *testing.T) {
	props := YamlConfigurations{
		"sql1": {
			"url": "123",
		},
	}
	got := replaceConfigurations("sql", props)
	require.Equal(t, YamlConfigurations{
		"sql1": {
			"dburl": "123",
		},
	}, got)
	props = YamlConfigurations{
		"kafka1": {
			"saslPassword": "123",
		},
	}
	got = replaceConfigurations("kafka", props)
	require.Equal(t, YamlConfigurations{
		"kafka1": {
			"password": "123",
		},
	}, got)
}

func setupConnectionYamlTest(t *testing.T) string {
	t.Helper()
	// These tests mutate global env/config cache state; do not mark them t.Parallel().
	require.NoError(t, conf.ClearKVStorage())
	t.Cleanup(func() {
		require.NoError(t, conf.ClearKVStorage())
	})

	baseDir := t.TempDir()
	t.Setenv(conf.KuiperBaseKey, baseDir)
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "etc", "connections"), os.ModePerm))

	confDir, err := conf.GetConfLoc()
	require.NoError(t, err)
	yamlPath := filepath.Join(confDir, "connections", "connection.yaml")
	delete(conf.LoadConfigCache, yamlPath)
	t.Cleanup(func() {
		delete(conf.LoadConfigCache, yamlPath)
	})
	return yamlPath
}

func writeConnectionYaml(t *testing.T, yamlPath, content string) {
	t.Helper()
	delete(conf.LoadConfigCache, yamlPath)
	require.NoError(t, os.WriteFile(yamlPath, []byte(content), 0o644))
}

func TestLoadConfigOperatorForConnectionYamlOps(t *testing.T) {
	yamlPath := setupConnectionYamlTest(t)

	t.Run("default create seeds missing connection and leaves API connection alone", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		// REST/API-created connection not present in YAML
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "usercreated", map[string]interface{}{
			"server": "tcp://user:1883",
		}))
		writeConnectionYaml(t, yamlPath, `mqtt:
  cloud:
    server: "tcp://broker:1883"
    username: ekuiper
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "")
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{
			"server":   "tcp://broker:1883",
			"username": "ekuiper",
		}, got["connections.mqtt.cloud"])
		require.Equal(t, map[string]interface{}{
			"server": "tcp://user:1883",
		}, got["connections.mqtt.usercreated"])
	})

	t.Run("create does not overwrite existing KV on reload", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "cloud", map[string]interface{}{
			"server": "tcp://existing:1883",
		}))
		writeConnectionYaml(t, yamlPath, `mqtt:
  cloud:
    xOperation: create
    server: "tcp://yaml:1883"
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "cloud")
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"server": "tcp://existing:1883"}, got["connections.mqtt.cloud"])
	})

	t.Run("delete removes target and does not remove unrelated API connection", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "cloud", map[string]interface{}{
			"server": "tcp://remote:1883",
		}))
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "usercreated", map[string]interface{}{
			"server": "tcp://user:1883",
		}))
		writeConnectionYaml(t, yamlPath, `mqtt:
  cloud:
    xOperation: delete
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "")
		require.NoError(t, err)
		_, cloudExists := got["connections.mqtt.cloud"]
		require.False(t, cloudExists, "cloud should be deleted by xOperation")
		require.Equal(t, map[string]interface{}{"server": "tcp://user:1883"}, got["connections.mqtt.usercreated"])
	})

	t.Run("delete missing connection is success no-op", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		writeConnectionYaml(t, yamlPath, `mqtt:
  missing:
    xOperation: delete
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "")
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("xOperation is not written into KV props", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		writeConnectionYaml(t, yamlPath, `mqtt:
  local:
    xOperation: create
    server: "tcp://127.0.0.1:1883"
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "local")
		require.NoError(t, err)
		props := got["connections.mqtt.local"]
		require.NotNil(t, props)
		_, hasOp := props["xoperation"]
		require.False(t, hasOp)
		_, hasOpCamel := props["xOperation"]
		require.False(t, hasOpCamel)
		require.Equal(t, "tcp://127.0.0.1:1883", props["server"])

		cfgOps, ok := GetConfOperator("connections.mqtt")
		require.True(t, ok)
		liveProps := cfgOps.CopyConfContent()["local"]
		require.NotContains(t, liveProps, "xoperation")
		require.NotContains(t, liveProps, "xOperation")
		require.Equal(t, "tcp://127.0.0.1:1883", liveProps["server"])
	})

	t.Run("unknown operation skips only that entry", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "keep", map[string]interface{}{
			"server": "tcp://keep:1883",
		}))
		writeConnectionYaml(t, yamlPath, `mqtt:
  keep:
    xOperation: delete
  bad:
    xOperation: upsert
    server: "tcp://bad:1883"
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "")
		require.NoError(t, err)
		_, keepExists := got["connections.mqtt.keep"]
		require.False(t, keepExists)
		_, badExists := got["connections.mqtt.bad"]
		require.False(t, badExists)
	})

	t.Run("operation value is case sensitive", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		writeConnectionYaml(t, yamlPath, `mqtt:
  cloud:
    xOperation: Delete
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "")
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("non-string operation skips only that entry", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		writeConnectionYaml(t, yamlPath, `mqtt:
  local:
    server: "tcp://127.0.0.1:1883"
  bad:
    xOperation: 1
    server: "tcp://bad:1883"
`)
		loadConfigOperatorForConnection("mqtt")

		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "")
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"server": "tcp://127.0.0.1:1883"}, got["connections.mqtt.local"])
		_, badExists := got["connections.mqtt.bad"]
		require.False(t, badExists)
	})

	t.Run("null entry is rejected and does not delete", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "cloud", map[string]interface{}{
			"server": "tcp://remote:1883",
		}))
		writeConnectionYaml(t, yamlPath, `mqtt:
  cloud: null
`)
		// NewConfigOperatorFromConnectionStorage rejects non-map entries; load is a no-op.
		loadConfigOperatorForConnection("mqtt")
		got, err := conf.GetCfgFromKVStorage("connections", "mqtt", "cloud")
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"server": "tcp://remote:1883"}, got["connections.mqtt.cloud"])
	})

	t.Run("delete entry is not exposed as live connection config", func(t *testing.T) {
		require.NoError(t, conf.ClearKVStorage())
		require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "cloud", map[string]interface{}{
			"server": "tcp://remote:1883",
		}))
		writeConnectionYaml(t, yamlPath, `mqtt:
  cloud:
    xOperation: delete
`)
		loadConfigOperatorForConnection("mqtt")

		cfgOps, ok := GetConfOperator("connections.mqtt")
		require.True(t, ok)
		_, exists := cfgOps.CopyConfContent()["cloud"]
		require.False(t, exists)
	})
}
