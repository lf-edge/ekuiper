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
