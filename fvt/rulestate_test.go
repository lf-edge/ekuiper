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

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
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

func (s *RuleStateTestSuite) TestMultipleStart() {
	topic := "testmul"
	subCh := pubsub.CreateSub(topic, nil, topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, topic)
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
  "triggered": false,
  "sql": "SELECT * FROM simStream",
  "actions": [
    {
      "memory":{"topic": "testmul"}
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
	})
	s.Run("start twice", func() {
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := client.StartRule("rule1")
				s.Require().NoError(err)
				s.Require().Equal(http.StatusOK, resp.StatusCode)
			}()
		}
		wg.Wait()
	})
	s.Run("check topo", func() {
		time.Sleep(50 * time.Millisecond)
		metrics, err := client.GetRuleStatus("rule1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		sinkout, existed := metrics["sink_memory_0_0_records_in_total"]
		s.Require().True(existed)
		s.Require().True(sinkout.(float64) > 5)
		s.T().Log(metrics)
		for i := 0; i < 5; i++ {
			<-subCh
			fmt.Printf("receive data %d\n", i)
		}
	})
	s.Run("stop rule", func() {
		resp, err := client.StopRule("rule1")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		noData := false
		for i := 0; i < 5; i++ {
			select {
			case <-subCh:
				fmt.Println("receive data")
			case <-time.After(time.Second):
				noData = true
			}
		}
		s.Require().True(noData)
	})
	s.Run("clean up", func() {
		res, e := client.Delete("rules/rule1")
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
