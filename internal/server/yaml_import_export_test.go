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

package server

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

func TestMetaConfiguration(t *testing.T) {
	conf.InitConf()
	conf.IsTesting = true
	connection.InitConnectionManager4Test()
	for _, v := range components {
		v.register()
	}
	InitConfManagers()
	prepare(t)
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/mockYamlExport", "return(true)")
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/mockYamlExport")
	m, err := GenMetaConfiguration()
	require.NoError(t, err)
	require.True(t, len(m.SourceConfig) > 0)
	require.True(t, len(m.SinkConfig) > 0)
	require.True(t, len(m.ConnectionConfig) > 0)
	require.True(t, len(m.PortablePlugins) > 0)
	require.True(t, len(m.Service) > 0)
	require.True(t, len(m.Schema) > 0)
	require.True(t, len(m.Uploads) > 0)
	require.True(t, len(m.Streams) > 0)
	require.True(t, len(m.Rules) > 0)
}

func prepare(t *testing.T) {
	deleteRule("metaConf")
	ruleProcessor.ExecDrop("metaConf")
	streamProcessor.ExecStreamSql(`drop stream metaConfTest`)
	require.NoError(t, conf.WriteCfgIntoKVStorage("sources", "mqtt", "demo1", map[string]any{
		"a": 1,
	}))
	require.NoError(t, conf.WriteCfgIntoKVStorage("sinks", "mqtt", "demo1", map[string]any{
		"a": 1,
	}))
	require.NoError(t, conf.WriteCfgIntoKVStorage("connections", "mqtt", "demo1", map[string]any{
		"a": 1,
	}))
	_, err := streamProcessor.ExecStreamSql(`create stream metaConfTest() WITH (DATASOURCE="/API/DATA",CONF_KEY="demo1")`)
	require.NoError(t, err)
	rulejson := `{"trigger":false,"id":"metaConf","sql":"select * from metaConfTest","actions":[{"log":{}}]}`
	_, err = createRule("metaConf", rulejson)
	require.NoError(t, err)
}
