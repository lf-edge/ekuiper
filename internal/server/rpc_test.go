// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/model"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/native"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/service"
)

type ServerTestSuite struct {
	suite.Suite
	s *Server
}

func (suite *ServerTestSuite) SetupTest() {
	conf.IsTesting = true
	suite.s = new(Server)
	nativeManager, _ = native.InitManager()
	portableManager, _ = portable.InitManager()
	serviceManager, _ = service.InitManager()
	_ = schema.InitRegistry()
	meta.InitYamlConfigManager()
}

func (suite *ServerTestSuite) TestStream() {
	sql := `Create Stream test () WITH (FORMAT="JSON", type="simulator");`
	var reply string
	err := suite.s.Stream(sql, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Stream test is created.\n", reply)

	reply = ""
	sql = "show streams;"
	err = suite.s.Stream(sql, &reply)
	assert.Nil(suite.T(), err)

	reply = ""
	sql = "SELECT * FROM test;"
	err = suite.s.CreateQuery(sql, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Query was submit successfully.", reply)

	var result string
	for i := 0; i < 5; i++ {
		var queryresult string
		time.Sleep(time.Second)
		err = suite.s.GetQueryResult("test", &queryresult)
		assert.Nil(suite.T(), err)
		if queryresult != "" {
			result += queryresult
			break
		}
	}
	allResults := strings.Split(result, "\n")
	assert.True(suite.T(), len(allResults) >= 1)
	assert.Equal(suite.T(), "[{\"humidity\":50,\"temperature\":22.5}]", allResults[0])
	stopQuery()
}

func (suite *ServerTestSuite) TestRule() {
	sql := `Create Stream test () WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
	var reply string
	err := suite.s.Stream(sql, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Stream test is created.\n", reply)

	reply = ""
	rule := `{
			  "sql": "SELECT * from test;",
			  "actions": [{
				"file": {
				  "path": "../internal/server/rpc_test_data/data/result.txt",
				  "interval": 5000,
				  "fileType": "lines",
				  "format": "json"
				}
			  }]
	}`
	ruleId := "myRule"
	args := &model.RPCArgDesc{Name: ruleId, Json: rule}
	err = suite.s.ValidateRule(args, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "The rule has been successfully validated and is confirmed to be correct.", reply)

	reply = ""
	rule = `{
			  "sql": "SELECT * from test;"
			}`
	args = &model.RPCArgDesc{Name: ruleId, Json: rule}
	err = suite.s.ValidateRule(args, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "invalid rule json: Missing rule actions.", reply)

	reply = ""
	rule = `{
			  "sql": "SELECT * from test;",
			  "actions": [{
				"file": {
				  "path": "../internal/server/rpc_test_data/data/result.txt",
				  "interval": 5000,
				  "fileType": "lines",
				  "format": "json"
				}
			  }]
	}`
	args = &model.RPCArgDesc{Name: ruleId, Json: rule}
	err = suite.s.CreateRule(args, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule was created successfully, please use 'bin/kuiper getstatus rule myRule' command to get rule status.", reply)

	reply = ""
	err = suite.s.GetStatusRule(ruleId, &reply)
	assert.Nil(suite.T(), err)

	reply = ""
	err = suite.s.ShowRules(1, &reply)
	assert.Nil(suite.T(), err)

	reply = ""
	err = suite.s.DescRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "{\n  \"triggered\": true,\n  \"id\": \"myRule\",\n  \"sql\": \"SELECT * from test;\",\n  \"actions\": [\n    {\n      \"file\": {\n        \"fileType\": \"lines\",\n        \"format\": \"json\",\n        \"interval\": 5000,\n        \"path\": \"../internal/server/rpc_test_data/data/result.txt\"\n      }\n    }\n  ],\n  \"options\": {\n    \"debug\": false,\n    \"isEventTime\": false,\n    \"lateTolerance\": \"1s\",\n    \"concurrency\": 1,\n    \"bufferLength\": 1024,\n    \"sendMetaToSink\": false,\n    \"sendNilField\": false,\n    \"sendError\": false,\n    \"checkpointInterval\": \"5m0s\",\n    \"restartStrategy\": {}\n  }\n}\n", reply)

	reply = ""
	err = suite.s.GetTopoRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "{\n  \"sources\": [\n    \"source_test\"\n  ],\n  \"edges\": {\n    \"op_2_decoder\": [\n      \"op_3_project\"\n    ],\n    \"op_3_project\": [\n      \"op_file_0_0_transform\"\n    ],\n    \"op_file_0_0_transform\": [\n      \"op_file_0_1_encode\"\n    ],\n    \"op_file_0_1_encode\": [\n      \"sink_file_0\"\n    ],\n    \"source_test\": [\n      \"op_2_decoder\"\n    ]\n  }\n}", reply)

	reply = ""
	err = suite.s.StopRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule was stopped.", reply)
	fmt.Println("rule stopped")

	reply = ""
	err = suite.s.StartRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule was started", reply)
	fmt.Println("rule started")

	reply = ""
	err = suite.s.RestartRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule was restarted.", reply)
	fmt.Println("rule restarted")

	reply = ""
	err = suite.s.DropRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule is dropped.", reply)
}

func (suite *ServerTestSuite) TestImportAndExport() {
	file := "rpc_test_data/import.json"
	var reply string
	err := suite.s.Import(file, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "imported 1 streams, 0 tables and 1 rules", reply)

	reply = ""
	file = "rpc_test_data/export.json"
	err = suite.s.Export(file, &reply)
	assert.Nil(suite.T(), err)
	os.Remove(file)
}

func (suite *ServerTestSuite) TestConfiguration() {
	importArg := model.ImportDataDesc{
		FileName: "rpc_test_data/import_configuration.json",
		Stop:     false,
		Partial:  false,
	}
	var reply string
	err := suite.s.ImportConfiguration(&importArg, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "{\n  \"ErrorMsg\": \"\",\n  \"ConfigResponse\": {\n    \"streams\": {},\n    \"tables\": {},\n    \"rules\": {},\n    \"nativePlugins\": {},\n    \"portablePlugins\": {},\n    \"sourceConfig\": {},\n    \"sinkConfig\": {},\n    \"connectionConfig\": {},\n    \"Service\": {},\n    \"Schema\": {},\n    \"uploads\": {},\n    \"scripts\": {}\n  }\n}", reply)

	reply = ""
	err = suite.s.GetStatusImport(1, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "{\n  \"streams\": {},\n  \"tables\": {},\n  \"rules\": {},\n  \"nativePlugins\": {},\n  \"portablePlugins\": {},\n  \"sourceConfig\": {},\n  \"sinkConfig\": {},\n  \"connectionConfig\": {},\n  \"Service\": {},\n  \"Schema\": {},\n  \"uploads\": {},\n  \"scripts\": {}\n}", reply)

	reply = ""
	exportArg := model.ExportDataDesc{
		FileName: "rpc_test_data/export_configuration.json",
		Rules:    []string{},
	}
	err = suite.s.ExportConfiguration(&exportArg, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "export configuration success", reply)
	os.Remove("rpc_test_data/export_configuration.json")
}

func (suite *ServerTestSuite) TearDownTest() {
	// Clean up
	sql := "DROP STREAM test;"
	var reply string
	_ = suite.s.Stream(sql, &reply)
	_ = suite.s.DropRule("myRule", &reply)
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
