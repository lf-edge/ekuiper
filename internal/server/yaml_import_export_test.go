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
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
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

func TestYamlImport(t *testing.T) {
	conf.InitConf()
	conf.IsTesting = true
	connection.InitConnectionManager4Test()
	for _, v := range components {
		v.register()
	}
	InitConfManagers()

	file := "./rpc_test_data/yaml_import.yaml"
	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()
	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, f)
	require.NoError(t, err)

	content := buffer.Bytes()
	require.NoError(t, importFromByte(content))

	got, err := conf.GetCfgFromKVStorage("sources", "mqtt", "demoImport")
	require.NoError(t, err)
	require.Len(t, got, 1)

	got, err = conf.GetCfgFromKVStorage("sinks", "mqtt", "demoImport")
	require.NoError(t, err)
	require.Len(t, got, 1)

	got, err = conf.GetCfgFromKVStorage("connections", "mqtt", "demoImport")
	require.NoError(t, err)
	require.Len(t, got, 1)

	_, err = streamProcessor.GetStream("demoImport", ast.TypeStream)
	require.NoError(t, err)

	_, err = streamProcessor.GetStream("helloImport", ast.TypeTable)
	require.NoError(t, err)

	r, err := ruleProcessor.GetRuleById("ruleImport")
	require.NoError(t, err)
	require.NotNil(t, r)
}

func TestYamlImportErr(t *testing.T) {
	conf.InitConf()
	conf.IsTesting = true
	connection.InitConnectionManager4Test()
	for _, v := range components {
		v.register()
	}
	InitConfManagers()

	file := "./rpc_test_data/yaml_import.yaml"
	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()
	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, f)
	require.NoError(t, err)
	content := buffer.Bytes()

	for v := mockErrStart + 1; v < mockErrEnd; v++ {
		failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/mockImportErr", fmt.Sprintf("return(%v)", v))
		require.Error(t, importFromByte(content))
	}
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/mockImportErr")
}
