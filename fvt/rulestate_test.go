// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"fmt"
	"net/http"
	"sync"
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
  "version": "123456",
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
		// test upsert
		resp, err = client.UpdateRule("rule1", ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		// test upsert with lower version
		ruleSql = `{
  "id": "rule1",
  "name": "keep rule",
  "version": "023456",
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
		resp, err = client.UpdateRule("rule1", ruleSql)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

		resp, err = client.Get("rules/rule1")
		s.Require().NoError(err)
		m, err := GetResponseResultMap(resp)
		s.Require().NoError(err)
		vv, ok := m["version"]
		s.Require().True(ok)
		version := "123456"
		s.Require().Equal(version, vv)

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
		// upsert stopped rule
		ruleSql3 := `{
  "triggered": false,
  "id": "rule3",
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
		resp, err = client.UpdateRule("rule3", ruleSql3)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		metrics, err := client.GetRuleStatus("rule3")
		s.Require().NoError(err)
		s.Equal("stopped", metrics["status"])
		s.T().Log(metrics)

		resp, err = client.StartRule("rule3")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		time.Sleep(50 * time.Millisecond)
		metrics, err = client.GetRuleStatus("rule3")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
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
		// Get rule3 metrics
		metrics, err = client.GetRuleStatus("rule3")
		sinkOut, ok := metrics["sink_nop_0_0_records_out_total"]
		s.True(ok)
		s.Require().True(sinkOut.(float64) > 0)
		s.Require().NoError(err)
	})
	s.Run("clean up", func() {
		res, e := client.Delete("rules/rule3")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/rule2")
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

func (s *RuleStateTestSuite) TestRuleTags() {
	s.Run("clean up", func() {
		client.DeleteStream("simStream1")
		client.DeleteRule("ruleTags")
	})
	s.Run("create rule and attach labels", func() {
		conf := map[string]any{
			"interval": "10ms",
		}
		resp, err := client.CreateConf("sources/simulator/confKeys/ttt", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql": "create stream simStream1() WITH (TYPE=\"simulator\", FORMAT=\"json\", CONF_KEY=\"ttt\", SHARED=\"true\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		ruleJson := `{
  "id": "ruleTags",
  "triggered": false,
  "sql": "SELECT * FROM simStream1",
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
		resp, err = client.CreateRule(ruleJson)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		resp, err = client.AddRuleTags("ruleTags", []string{"t1", "t2"})
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		lists, err := client.GetRulesByTags([]string{"t1", "t2"})
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		s.Require().Equal([]string{"ruleTags"}, lists)

		resp, err = client.RemoveRuleTags("ruleTags", []string{"t1"})
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		lists, err = client.GetRulesByTags([]string{"t1", "t2"})
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		s.Require().Equal([]string{}, lists)

		resp, err = client.ResetRuleTags("ruleTags", []string{"t1", "t2"})
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		lists, err = client.GetRulesByTags([]string{"t1", "t2"})
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		s.Require().Equal([]string{"ruleTags"}, lists)
	})
	s.Run("clean up", func() {
		client.DeleteStream("simStream1")
		client.DeleteRule("ruleTags")
	})
}

// Test two rules with shared stream
func (s *RuleStateTestSuite) TestMulShared() {
	ruleSql := `{
  "id": "mul1",
  "sql": "SELECT * FROM sims",
  "actions": [
    {
      "nop":{}
    }
  ],
  "options": {
    "sendError": false
  }
}`
	ruleSql2 := `{
  "id": "mul2",
  "sql": "SELECT * FROM sims",
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
	// Start two rules
	s.Run("init rules", func() {
		conf := map[string]any{
			"interval": "10ms",
		}
		resp, err := client.CreateConf("sources/simulator/confKeys/mul1", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql": "create stream sims() WITH (TYPE=\"simulator\", FORMAT=\"json\", CONF_KEY=\"mul1\", SHARED=\"true\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		resp, err = client.CreateRule(ruleSql2)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	// Chain start/stop
	s.Run("continuous start/stop", func() {
		metrics, err := client.GetRuleStatus("mul1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)

		resp, err := client.StartRule("mul1")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		time.Sleep(50 * time.Millisecond)
		metrics, err = client.GetRuleStatus("mul1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)

		resp, err = client.StopRule("mul1")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		time.Sleep(50 * time.Millisecond)
		metrics, err = client.GetRuleStatus("mul1")
		s.Require().NoError(err)
		s.Equal("stopped", metrics["status"])
		s.T().Log(metrics)

		resp, err = client.StartRule("mul1")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		time.Sleep(50 * time.Millisecond)
		metrics, err = client.GetRuleStatus("mul1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
	})
	// Chain update
	s.Run("chain update", func() {
		resp, err := client.UpdateRule("mul1", ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		metrics, err := client.GetRuleStatus("mul1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)

		resp, err = client.UpdateRule("mul2", ruleSql2)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		metrics, err = client.GetRuleStatus("mul2")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
	})
	// Async update and start/stop
	s.Run("async run", func() {
		wg := sync.WaitGroup{}
		wg.Add(6)
		final := 0
		go func() {
			defer wg.Done()
			fmt.Println("start 1")
			resp, err := client.StartRule("mul1")
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			final = 0
		}()
		go func() {
			defer wg.Done()
			fmt.Println("stop 1")
			resp, err := client.StopRule("mul1")
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			final = 1
		}()
		go func() {
			defer wg.Done()
			fmt.Println("update 1")
			resp, err := client.UpdateRule("mul1", ruleSql)
			s.Require().NoError(err)
			s.T().Log(GetResponseText(resp))
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			final = 2
		}()
		go func() {
			defer wg.Done()
			fmt.Println("update 2")
			resp, err := client.UpdateRule("mul2", ruleSql2)
			s.Require().NoError(err)
			s.T().Log(GetResponseText(resp))
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			final = 3
		}()
		go func() {
			defer wg.Done()
			fmt.Println("stop 2")
			resp, err := client.StopRule("mul1")
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			final = 4
		}()
		go func() {
			defer wg.Done()
			fmt.Println("start 2")
			resp, err := client.StartRule("mul1")
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			final = 5
		}()
		wg.Wait()
		metrics, err := client.GetRuleStatus("mul2")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		exp, ok := metrics["source_sims_0_exceptions_total"]
		s.True(ok)
		s.Require().True(exp.(float64) == 0)
		sinkOut1, ok := metrics["source_sims_0_records_in_total"]
		s.True(ok)
		s.True(sinkOut1.(float64) > 0)
		// mul1 status depends on the final command
		metrics, err = client.GetRuleStatus("mul1")
		s.Require().NoError(err)
		fmt.Println("final", final)
		if final == 1 || final == 4 {
			s.Equal("stopped", metrics["status"])
		} else {
			s.Equal("running", metrics["status"])
		}
	})
	// Clean
	s.Run("clean up", func() {
		res, e := client.Delete("rules/mul2")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/mul1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/sims")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}
