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

package fvt

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ImportTestSuite struct {
	suite.Suite
}

func TestImportTestSuite(t *testing.T) {
	suite.Run(t, new(ImportTestSuite))
}

func (s *ImportTestSuite) TestImportError() {
	s.Run("partial import wrong content", func() {
		wrongContent := `{"content":"{\"streams\": {\"demo\": \"CREATE STwREAM demo () WITH (DATASOURCE=\\\"users\\\", CONF_KEY=\\\"td\\\",TYPE=\\\"none\\\", FORMAT=\\\"JSON\\\")\"}}"}`
		resp, err := client.PostWithParam("data/import", "partial=1", wrongContent)
		s.Require().NoError(err)
		s.Require().Equal(400, resp.StatusCode)
		result, err := GetResponseText(resp)
		s.Require().NoError(err)
		exp := "{\"error\":1000,\"message\":\"{\\\"streams\\\":{\\\"demo\\\":\\\"found \\\\\\\"STWREAM\\\\\\\", expected keyword stream or table.\\\"},\\\"tables\\\":{},\\\"rules\\\":{},\\\"nativePlugins\\\":{},\\\"portablePlugins\\\":{},\\\"sourceConfig\\\":{},\\\"sinkConfig\\\":{},\\\"connectionConfig\\\":{},\\\"Service\\\":{},\\\"Schema\\\":{},\\\"uploads\\\":{},\\\"scripts\\\":{}}\"}\n"

		s.Require().Equal(exp, result)
	})

	s.Run("full import wrong content", func() {
		wrongContent := `{"content":"{\"streams\": {\"demo\": \"CREATE STwREAM demo () WITH (DATASOURCE=\\\"users\\\", CONF_KEY=\\\"td\\\",TYPE=\\\"none\\\", FORMAT=\\\"JSON\\\")\"}}"}`
		resp, err := client.Post("data/import", wrongContent)
		s.Require().NoError(err)
		s.Require().Equal(400, resp.StatusCode)
		result, err := GetResponseText(resp)
		s.Require().NoError(err)
		exp := "{\"error\":1000,\"message\":\"{\\\"streams\\\":{\\\"demo\\\":\\\"found \\\\\\\"STWREAM\\\\\\\", expected keyword stream or table.\\\"},\\\"tables\\\":{},\\\"rules\\\":{},\\\"nativePlugins\\\":{},\\\"portablePlugins\\\":{},\\\"sourceConfig\\\":{},\\\"sinkConfig\\\":{},\\\"connectionConfig\\\":{},\\\"Service\\\":{},\\\"Schema\\\":{},\\\"uploads\\\":{},\\\"scripts\\\":{}}\"}\n"

		s.Require().Equal(exp, result)
	})

	s.Run("full import wrong json", func() {
		wrongContent := `{"content":"{\"streams\": {\"demo\": \"CREATE STwREAM demo () WITH (DATASOURCE=\\\"users\\\", CONF_KEY=\\\"td\\\",TYPE=\\\"none\\\", FORMAT=\\\"JSO"}`
		resp, err := client.Post("data/import", wrongContent)
		s.Require().NoError(err)
		s.Require().Equal(400, resp.StatusCode)
		result, err := GetResponseText(resp)
		s.Require().NoError(err)
		exp := "{\"error\":1000,\"message\":\"configuration unmarshal with error unexpected end of JSON input\"}\n"

		s.Require().Equal(exp, result)
	})
}
