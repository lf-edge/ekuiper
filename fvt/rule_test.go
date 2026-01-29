// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"os"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/server"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type RuleTestSuite struct {
	suite.Suite
}

func TestRuleSuite(t *testing.T) {
	suite.Run(t, new(RuleTestSuite))
}

func (s *RuleTestSuite) TestRuleDisableBufferFullDiscard() {
	topic := "test1"
	subCh := pubsub.CreateSub(topic, nil, topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, topic)
	data := []map[string]any{
		{
			"a": float64(1),
		},
		{
			"a": float64(2),
		},
		{
			"a": float64(3),
		},
		{
			"a": float64(4),
		},
		{
			"a": float64(5),
		},
		{
			"a": float64(6),
		},
	}
	conf := map[string]any{
		"data":     data,
		"interval": "1ms",
		"loop":     false,
	}
	resp, err := client.CreateConf("sources/simulator/confKeys/sim1", conf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	streamSql := `{"sql": "create stream sim1() WITH (TYPE=\"simulator\", CONF_KEY=\"sim1\")"}`
	resp, err = client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	ruleSql := `{
  "id": "ruleSim1",
  "sql": "SELECT * FROM sim1",
  "actions": [
    {
      "memory":{
        "topic": "test1",
        "bufferLength": 1
      }
    }
  ],
  "options": {
    "disableBufferFullDiscard": true,
    "bufferLength": 1
  }
}`
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	s.assertRecvMemTuple(subCh, data)
}

func (s *RuleTestSuite) assertRecvMemTuple(subCh chan any, expect []map[string]any) {
	for _, e := range expect {
		d := <-subCh
		mt, ok := d.([]pubsub.MemTuple)
		s.Require().True(ok)
		s.Require().Len(mt, 1)
		s.Require().Equal(e, mt[0].ToMap())
	}
}

func (s *RuleTestSuite) TestUpsert() {
	topic := "sim/#"
	server := mqtt.New(&mqtt.Options{InlineClient: true})
	defer server.Close()
	result := make(map[string]string)
	lock := syncx.Mutex{}
	s.Run("start broker and subscribe for result", func() {
		// Allow all connections.
		_ = server.AddHook(new(auth.AllowHook), nil)
		// Create a TCP listener on a standard port.
		tcp := listeners.NewTCP(listeners.Config{ID: "upsert0", Address: ":4883"})
		err := server.AddListener(tcp)
		s.Require().NoError(err)
		go func() {
			err = server.Serve()
			fmt.Println(err)
		}()
		fmt.Println(tcp.Address())
		err = server.Subscribe(topic, 1, func(cl *mqtt.Client, sub packets.Subscription, pk packets.Packet) {
			lock.Lock()
			defer lock.Unlock()
			result[pk.TopicName] = string(pk.Payload)
			if len(result) == 4 {
				server.Unsubscribe(topic, 1)
			}
		})
		s.Require().NoError(err)
	})
	s.Run("import initial rules and stop 1", func() {
		oldContent, err := os.ReadFile("rules/old.json")
		s.Require().NoError(err)
		resp, err := client.Import(string(oldContent))
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(200 * time.Millisecond)
		// check metrics
		metrics, err := client.GetRuleStatus("hot")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		sinkOut1, ok := metrics["sink_mqtt_0_0_records_out_total"]
		s.True(ok)
		s.Require().True(sinkOut1.(float64) > 10)
		// Get 2nd metrics
		metrics, err = client.GetRuleStatus("cold")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		sinkOut2, ok := metrics["sink_mqtt_0_0_records_out_total"]
		s.True(ok)
		s.Require().True(sinkOut2.(float64) > 10)
		// stop the cold rule
		resp, err = client.StopRule("cold")
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(10 * time.Millisecond)
		metrics, err = client.GetRuleStatus("cold")
		s.Require().NoError(err)
		s.Equal("stopped", metrics["status"])
	})
	s.Run("import two rules immediately", func() {
		new1Content, err := os.ReadFile("rules/new1.json")
		s.Require().NoError(err)
		new2Content, err := os.ReadFile("rules/new2.json")
		s.Require().NoError(err)
		resp1, err1 := client.Import(string(new1Content))
		resp2, err2 := client.Import(string(new2Content))
		s.Require().NoError(err1)
		s.Require().Equal(http.StatusOK, resp1.StatusCode)
		s.Require().NoError(err2)
		s.Require().Equal(http.StatusOK, resp2.StatusCode)
		// wait and get the metrics
		time.Sleep(200 * time.Millisecond)
		// check metrics
		metrics, err := client.GetRuleStatus("hot")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		sinkOut1, ok := metrics["sink_mqtt_0_0_records_out_total"]
		s.True(ok)
		s.Require().True(sinkOut1.(float64) > 10)
		connTime1, ok := metrics["source_simup_0_connection_last_connected_time"]
		s.True(ok)
		// Get 2nd metrics
		metrics, err = client.GetRuleStatus("cold")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		sinkOut2, ok := metrics["sink_mqtt_0_0_records_out_total"]
		s.True(ok)
		s.Require().True(sinkOut2.(float64) > 10)
		connTime2, ok := metrics["source_simup_0_connection_last_connected_time"]
		s.True(ok)
		s.Require().Equal(connTime1, connTime2)
	})
	s.Run("clean", func() {
		res, e := client.Delete("rules/cold")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/hot")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/simup")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
	s.Run("compare result", func() {
		expected := map[string]string{"sim/new1": "{\"b\":2}", "sim/new2": "{\"a\":1}", "sim/old1": "{\"a\":1}", "sim/old2": "{\"b\":2}"}
		s.Require().Equal(expected, result)
	})
}

func (s *RuleTestSuite) TestStreamSchema() {
	streamName := "test_stream_schema"
	ruleName := "test_rule_schema"
	defer client.DeleteStream(streamName)
	defer client.DeleteRule(ruleName)
	streamSql := fmt.Sprintf(`{"sql": "create stream %s(id bigint, name string, age string) WITH (TYPE=\"mqtt\",DATASOURCE=\"mock\")"}`, streamName)
	resp, err := client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	ruleSql := fmt.Sprintf(`{
		"id": "%s",
		"sql": "SELECT id, name FROM %s",
		"actions": [
			{
				"log":{}
			}
		]
	}`, ruleName, streamName)
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	schema, err := client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	expected := map[string]any{
		"age":  map[string]any{"type": "string", "index": float64(0)},
		"id":   map[string]any{"type": "bigint", "index": float64(0)},
		"name": map[string]any{"type": "string", "index": float64(0)},
	}
	s.Require().Equal(expected, schema)
}

func (s *RuleTestSuite) TestBatchRequest() {
	client.DeleteStream("demobatch")
	reqs := make([]*server.EachRequest, 0)
	reqs = append(reqs, &server.EachRequest{
		Method: "POST",
		Path:   "/streams",
		Body:   "{\"sql\":\"CREATE stream demobatch() WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}",
	})
	reqs = append(reqs, &server.EachRequest{
		Method: "GET",
		Path:   "/streams/demobatch",
	})
	resps, err := client.BatchRequest(reqs)
	s.Require().NoError(err)
	s.Require().Len(resps, len(reqs))
	s.Require().Equal(http.StatusCreated, resps[0].Code)
	s.Require().Equal(http.StatusOK, resps[1].Code)
}

func (s *RuleTestSuite) TestStreamSchemaWithSharedSource() {
	streamName := "test_stream_schema_shared"
	rule1 := "rule1"
	rule2 := "rule2"
	defer client.DeleteStream(streamName)
	defer client.DeleteRule(rule1)
	defer client.DeleteRule(rule2)
	streamSql := fmt.Sprintf(`{"sql": "create stream %s() WITH (TYPE=\"mqtt\", DATASOURCE=\"test\", SHARED=\"true\")"}`, streamName)
	resp, err := client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	ruleSql1 := fmt.Sprintf(`{
		"id": "%s",
		"sql": "SELECT id, name FROM %s",
		"actions": [
			{
				"log":{}
			}
		]
	}`, rule1, streamName)
	resp, err = client.CreateRule(ruleSql1)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// wait until rule starts to run
	time.Sleep(100 * time.Millisecond)

	schema, err := client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	expected1 := map[string]any{
		"id":   nil,
		"name": nil,
	}
	s.Require().Equal(expected1, schema)

	ruleSql2 := fmt.Sprintf(`{
		"id": "%s",
		"sql": "SELECT id, age FROM %s",
		"actions": [
			{
				"log":{}
			}
		]
	}`, rule2, streamName)
	resp, err = client.CreateRule(ruleSql2)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	time.Sleep(100 * time.Millisecond)

	schema, err = client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	expected2 := map[string]any{
		"id":   nil,
		"name": nil,
		"age":  nil,
	}
	s.Require().Equal(expected2, schema)

	resp, err = client.DeleteRule(rule2)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	time.Sleep(100 * time.Millisecond)

	schema, err = client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	s.Require().Equal(expected1, schema)
}

func (s *RuleTestSuite) TestStreamSliceSchemaWithSharedSource() {
	streamName := "test_stream_schema_shared"
	rule1 := "rule1"
	rule2 := "rule2"
	defer client.DeleteStream(streamName)
	defer client.DeleteRule(rule1)
	defer client.DeleteRule(rule2)
	streamSql := fmt.Sprintf(`{"sql": "create stream %s() WITH (TYPE=\"mqtt\", DATASOURCE=\"test\", SHARED=\"true\")"}`, streamName)
	resp, err := client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	ruleSql1 := fmt.Sprintf(`{
		"id": "%s",
		"sql": "SELECT id, name FROM %s",
		"actions": [
			{
				"log":{}
			}
		],
		"options": {
			"experiment": {
			  "useSliceTuple": true
			}
	  	}
	}`, rule1, streamName)
	resp, err = client.CreateRule(ruleSql1)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// wait until rule starts to run
	time.Sleep(100 * time.Millisecond)

	schema, err := client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	s.Require().Len(schema, 2)
	id, ok := schema["id"].(map[string]any)
	s.Require().True(ok)
	name, ok := schema["name"].(map[string]any)
	s.Require().True(ok)
	s.Require().Equal(true, id["hasIndex"])
	s.Require().Equal(true, name["hasIndex"])
	s.Require().ElementsMatch([]float64{0, 1}, []float64{id["index"].(float64), name["index"].(float64)})

	ruleSql2 := fmt.Sprintf(`{
		"id": "%s",
		"sql": "SELECT id, age FROM %s",
		"actions": [
			{
				"log":{}
			}
		],
		"options": {
			"experiment": {
			  "useSliceTuple": true
			}
	  	}
	}`, rule2, streamName)
	resp, err = client.CreateRule(ruleSql2)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	time.Sleep(100 * time.Millisecond)

	schema, err = client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	s.Require().Len(schema, 3)
	id, ok = schema["id"].(map[string]any)
	s.Require().True(ok)
	name, ok = schema["name"].(map[string]any)
	s.Require().True(ok)
	age, ok := schema["age"].(map[string]any)
	s.Require().True(ok)
	s.Require().Equal(true, id["hasIndex"])
	s.Require().Equal(true, name["hasIndex"])
	s.Require().Equal(true, age["hasIndex"])
	s.Require().ElementsMatch([]float64{0, 1, 2}, []float64{id["index"].(float64), name["index"].(float64), age["index"].(float64)})

	resp, err = client.DeleteRule(rule2)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	time.Sleep(100 * time.Millisecond)

	schema, err = client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	s.Require().Len(schema, 2)
	id, ok = schema["id"].(map[string]any)
	s.Require().True(ok)
	name, ok = schema["name"].(map[string]any)
	s.Require().True(ok)
	s.Require().ElementsMatch([]float64{0, 1}, []float64{id["index"].(float64), name["index"].(float64)})
}

func (s *RuleTestSuite) TestRuleSchema() {
	streamName := "test_stream_schema_rule"
	ruleName := "test_rule_schema_rule"
	defer client.DeleteStream(streamName)
	defer client.DeleteRule(ruleName)
	streamSql := fmt.Sprintf(`{"sql": "create stream %s(id bigint, name string, age string) WITH (TYPE=\"mqtt\",DATASOURCE=\"mock\")"}`, streamName)
	resp, err := client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	ruleSql := fmt.Sprintf(`{
		"id": "%s",
		"sql": "SELECT id, name FROM %s",
		"actions": [
			{
				"log":{}
			}
		]
	}`, ruleName, streamName)
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// wait until rule starts to run
	time.Sleep(100 * time.Millisecond)

	schema, err := client.GetRuleSchema(ruleName)
	s.Require().NoError(err)
	expected := map[string]any{
		"id":   map[string]any{"hasIndex": true, "index": float64(0)},
		"name": map[string]any{"hasIndex": true, "index": float64(1)},
	}
	s.Require().Equal(expected, schema)
}
