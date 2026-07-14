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
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestOverwriteProps(t *testing.T) {
	conf.IsTesting = true
	confProps := map[string]interface{}{"server": "123"}
	require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "conf1", confProps))
	oldProps := map[string]interface{}{
		"connectionSelector": "conf1",
	}
	newProps, err := OverwriteByConnectionConf("mqtt", oldProps)
	require.NoError(t, err)
	for k, v := range confProps {
		require.Equal(t, v, newProps[k])
	}
	for k, v := range oldProps {
		require.Equal(t, v, newProps[k])
	}

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/topo/node/conf/overwriteErr", `return(true)`)
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/topo/node/conf/overwriteErr")
	_, err = OverwriteByConnectionConf("mqtt", oldProps)
	require.Error(t, err)
}

func TestOverwriteKafkaConnectionProps(t *testing.T) {
	conf.IsTesting = true
	connProps := map[string]interface{}{
		"brokers":      "127.0.0.1:9092",
		"saslAuthType": "plain",
		"saslUserName": "user",
		"password":     "secret",
	}
	require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "kafka", "kafkaConn1", connProps))
	sinkProps := map[string]interface{}{
		"connectionSelector": "kafkaConn1",
		"topic":              "test-topic",
	}

	newProps, err := OverwriteByConnectionConf("kafka", sinkProps)
	require.NoError(t, err)
	require.Equal(t, "kafkaConn1", newProps["connectionSelector"])
	require.Equal(t, "test-topic", newProps["topic"])
	for k, v := range connProps {
		require.Equal(t, v, newProps[k])
	}
}

func TestOverwriteIgnoresOperationEntriesFromConnectionYaml(t *testing.T) {
	baseDir := t.TempDir()
	t.Setenv(conf.KuiperBaseKey, baseDir)
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "etc", "connections"), os.ModePerm))

	yamlPath := filepath.Join(baseDir, "etc", "connections", "connection.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(`operationtest:
  deleted:
    xOperation: delete
    server: tcp://deleted:1883
  rejected:
    xOperation: unsupported
    server: tcp://rejected:1883
  created:
    xOperation: create
    server: tcp://created:1883
  defaultCreate:
    server: tcp://default:1883
`), 0o644))
	delete(conf.LoadConfigCache, yamlPath)
	t.Cleanup(func() {
		delete(conf.LoadConfigCache, yamlPath)
	})

	for _, selector := range []string{"deleted", "rejected"} {
		t.Run(selector, func(t *testing.T) {
			props := map[string]interface{}{
				"connectionSelector": selector,
			}
			got, err := OverwriteByConnectionConf("operationtest", props)
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{
				"connectionSelector": selector,
			}, got)
		})
	}

	for selector, server := range map[string]string{
		"created":       "tcp://created:1883",
		"defaultcreate": "tcp://default:1883",
	} {
		t.Run(selector, func(t *testing.T) {
			got, err := OverwriteByConnectionConf("operationtest", map[string]interface{}{
				"connectionSelector": selector,
			})
			require.NoError(t, err)
			require.Equal(t, selector, got["connectionSelector"])
			require.Equal(t, server, got["server"])
			require.NotContains(t, got, "xoperation")
		})
	}
}
