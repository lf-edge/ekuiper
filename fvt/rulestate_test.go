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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type RuleStateTestSuite struct {
	suite.Suite
}

func TestRuleTestSuite(t *testing.T) {
	suite.Run(t, new(RuleStateTestSuite))
}

func (s *RuleStateTestSuite) TestUpdate() {
	s.Run("init rule1", func() {
		conf := map[string]any{
			"interval": "10ms",
		}
		resp, err := client.CreateConf("sources/simulator/confKeys/ttt", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql": "create stream simStream() WITH (TYPE=\"simulator\", FORMAT=\"json\", CONF_KEY=\"ttt\", SHARED=\"true\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "rule1",
  "name": "keep rule",
  "sql": "SELECT * FROM simStream",
  "actions": [
    {
      "nop":{}
    }
  ],
  "options": {
    "sendError": false
  }
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql2 := `{
  "id": "rule2",
  "name": "to update rule",
  "sql": "SELECT * FROM simStream",
  "actions": [
    {
      "nop":{}
    }
  ],
  "options": {
    "sendError": false,
	"bufferLength": 2
  }
}`
		resp, err = client.CreateRule(ruleSql2)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("stop and update rule2 but not start", func() {
		resp, err := client.StopRule("rule2")
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		ruleSql2 := `{
  "id": "rule2",
  "triggered": false,
  "name": "to update rule",
  "sql": "SELECT * FROM simStream",
  "actions": [
    {
      "nop":{}
    }
  ],
  "options": {
    "sendError": false,
	"bufferLength": 2
  }
}`
		resp, err = client.UpdateRule("rule2", ruleSql2)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	s.Run("check no buffer is not full exp", func() {
		// Get metrics
		metrics, err := client.GetRuleStatus("rule1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		exp, ok := metrics["source_simStream_0_exceptions_total"]
		s.True(ok)
		s.Require().True(exp.(float64) == 0)
		sinkOut1, ok := metrics["source_simStream_0_records_in_total"]
		s.True(ok)
		// Get 2nd metrics
		time.Sleep(50 * time.Millisecond)
		metrics, err = client.GetRuleStatus("rule1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		exp, ok = metrics["source_simStream_0_exceptions_total"]
		s.True(ok)
		s.Require().True(exp.(float64) == 0, "has exception")
		sinkOut2, ok := metrics["source_simStream_0_records_in_total"]
		s.True(ok)
		s.Require().True(sinkOut2.(float64)-sinkOut1.(float64) > 0)
	})
	s.Run("clean up", func() {
		res, e := client.Delete("rules/rule2")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/rule1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/simStream")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}

func (s *RuleStateTestSuite) TestCreateStoppedRule() {
	s.Run("init rule1", func() {
		conf := map[string]any{
			"interval": "10ms",
		}
		resp, err := client.CreateConf("sources/simulator/confKeys/ttt", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql": "create stream simStream() WITH (TYPE=\"simulator\", FORMAT=\"json\", CONF_KEY=\"ttt\", SHARED=\"true\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "rule1",
  "name": "keep rule",
  "sql": "SELECT * FROM simStream",
  "actions": [
    {
      "nop":{}
    }
  ],
  "options": {
    "sendError": false
  }
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql2 := `{
  "triggered": false,
  "id": "rule2",
  "name": "to update rule",
  "sql": "SELECT * FROM simStream",
  "actions": [
    {
      "nop":{}
    }
  ],
  "options": {
    "sendError": false,
	"bufferLength": 2
  }
}`
		resp, err = client.CreateRule(ruleSql2)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("check no buffer is not full exp", func() {
		// Get metrics
		metrics, err := client.GetRuleStatus("rule1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		exp, ok := metrics["source_simStream_0_exceptions_total"]
		s.True(ok)
		s.Require().True(exp.(float64) == 0)
		sinkOut1, ok := metrics["source_simStream_0_records_in_total"]
		s.True(ok)
		// Get 2nd metrics
		time.Sleep(50 * time.Millisecond)
		metrics, err = client.GetRuleStatus("rule1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		exp, ok = metrics["source_simStream_0_exceptions_total"]
		s.True(ok)
		s.Require().True(exp.(float64) == 0, "has exception")
		sinkOut2, ok := metrics["source_simStream_0_records_in_total"]
		s.True(ok)
		s.Require().True(sinkOut2.(float64)-sinkOut1.(float64) > 0)
	})
	s.Run("clean up", func() {
		res, e := client.Delete("rules/rule2")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/rule1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/simStream")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}
