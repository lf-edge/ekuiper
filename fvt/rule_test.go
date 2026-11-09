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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
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

func (s *RuleTestSuite) TestRuleAggInWhere() {
	topic := "test2"
	subCh := pubsub.CreateSub(topic, nil, topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, topic)
	data := []map[string]any{
		{
			"a": float64(1),
		},
		{
			"a": float64(2),
		},
	}
	conf := map[string]any{
		"data":     data,
		"interval": "1ms",
		"loop":     false,
	}
	resp, err := client.CreateConf("sources/simulator/confKeys/sim5", conf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	streamSql := `{"sql": "create stream sim5() WITH (TYPE=\"simulator\", CONF_KEY=\"sim5\")"}`
	resp, err = client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	ruleSql := `{
    "id": "ruleSim5",
    "sql": "SELECT * FROM sim5 where a > avg(a) group by countwindow(2)",
    "actions": [
        {
            "memory": {
                "topic": "test2"
            }
        }
    ],
    "options": {
        
    }
}`
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	s.assertRecvMemTuple(subCh, []map[string]any{{"a": float64(2)}})
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
	streamSql := `{"sql": "create stream sim1() WITH (TYPE=\"simulator\", CONF_KEY=\"sim1\", TEMP=\"true\")"}`
	resp, err = client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	ruleSql := `{
  "id": "ruleSim1",
  "sql": "SELECT * FROM sim1",
  "temp":true,
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

// TestAccMaxByMapAggChargeCycle verifies a two-rule vehicle charge-cycle flow.
// Rule 1 calculates the cumulative maximum temperature and its timestamp per SOC.
// Rule 2 receives Rule 1's output through a memory stream and emits the complete
// SOC map only when upload_flag is set.
func (s *RuleTestSuite) TestAccMaxByMapAggChargeCycle() {
	const (
		inputStream    = "accMapVehicleInput"
		statStream     = "accMapVehicleStat"
		statTopic      = "acc_map_vehicle_stat"
		resultTopic    = "acc_map_vehicle_result"
		ruleUpstream   = "accMapVehicleUpstream"
		ruleDownstream = "accMapVehicleDownstream"
	)
	client.DeleteRule(ruleUpstream)
	client.DeleteRule(ruleDownstream)
	client.DeleteStream(inputStream)
	client.DeleteStream(statStream)
	defer client.DeleteRule(ruleUpstream)
	defer client.DeleteRule(ruleDownstream)
	defer client.DeleteStream(inputStream)
	defer client.DeleteStream(statStream)

	subCh := pubsub.CreateSub(resultTopic, nil, resultTopic, 10)
	defer pubsub.CloseSourceConsumerChannel(resultTopic, resultTopic)

	data := []map[string]any{
		{"soc": 18, "temp": 28, "ts": 1788000000000},
		{"soc": 18, "temp": 29, "ts": 1788000030000},
		{"soc": 18, "temp": 30, "ts": 1788000060000},
		{"soc": 19, "temp": 31, "ts": 1788000090000},
		{"soc": 19, "temp": 32, "ts": 1788000120000},
		{"soc": 20, "temp": 31, "ts": 1788000150000},
		{"soc": 20, "temp": 32, "ts": 1788000180000},
		{"soc": 20, "temp": 33, "ts": 1788000210000},
		{"soc": 21, "temp": 34, "ts": 1788000240000},
		{"soc": 21, "temp": 35, "ts": 1788000270000},
		{"soc": 21, "temp": 35, "ts": 1788000300000},
		{"soc": 22, "temp": 34, "ts": 1788000330000},
		{"soc": 22, "temp": 33, "ts": 1788000690000, "upload_flag": 1},
	}
	resp, err := client.CreateConf("sources/simulator/confKeys/accMapVehicle", map[string]any{
		"data": data, "interval": "10ms", "loop": false,
	})
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	for _, sql := range []string{
		fmt.Sprintf(`{"sql":"CREATE STREAM %s (soc BIGINT, temp BIGINT, ts BIGINT, upload_flag BIGINT) WITH (TYPE=\"simulator\", FORMAT=\"json\", CONF_KEY=\"accMapVehicle\")"}`, inputStream),
		fmt.Sprintf(`{"sql":"CREATE STREAM %s () WITH (TYPE=\"memory\", DATASOURCE=\"%s\", FORMAT=\"json\")"}`, statStream, statTopic),
	} {
		resp, err = client.CreateStream(sql)
		s.Require().NoError(err)
		if resp.StatusCode != http.StatusCreated {
			body, readErr := GetResponseText(resp)
			s.Require().NoError(readErr)
			s.T().Logf("create stream response: %s", body)
		}
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	}

	// Start the downstream rule first so no intermediate memory messages are lost.
	rule := fmt.Sprintf(`{
  "id": "%s",
  "sql": "SELECT acc_map_agg(soc, object_construct('max_temp', max_temp, 'max_temp_ts', max_temp_ts)) AS data FROM %s WHERE upload_flag = 1",
  "actions": [{"memory": {"topic": "%s"}}]
}`, ruleDownstream, statStream, resultTopic)
	resp, err = client.CreateRule(rule)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	rule = fmt.Sprintf(`{
  "id": "%s",
  "sql": "SELECT soc, acc_max(temp) OVER (PARTITION BY soc) AS max_temp, acc_max_by(ts, temp) OVER (PARTITION BY soc) AS max_temp_ts, upload_flag FROM %s",
  "actions": [{"memory": {"topic": "%s"}}]
}`, ruleUpstream, inputStream, statTopic)
	resp, err = client.CreateRule(rule)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	expected := map[string]any{"data": []map[string]any{
		{"key": "18", "value": map[string]any{"max_temp": float64(30), "max_temp_ts": float64(1788000060000)}},
		{"key": "19", "value": map[string]any{"max_temp": float64(32), "max_temp_ts": float64(1788000120000)}},
		{"key": "20", "value": map[string]any{"max_temp": float64(33), "max_temp_ts": float64(1788000210000)}},
		{"key": "21", "value": map[string]any{"max_temp": float64(35), "max_temp_ts": float64(1788000300000)}},
		{"key": "22", "value": map[string]any{"max_temp": float64(34), "max_temp_ts": float64(1788000330000)}},
	}}
	s.assertRecvMemTuple(subCh, []map[string]any{expected})
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
			err := server.Serve()
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
		lock.Lock()
		defer lock.Unlock()
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
	expected1 := map[string]any{
		"id":   map[string]any{"hasIndex": true, "index": float64(0)},
		"name": map[string]any{"hasIndex": true, "index": float64(1)},
	}
	s.assertSchemaEquality(expected1, schema)

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
	expected2 := map[string]any{
		"id":   map[string]any{"hasIndex": true, "index": float64(0)},
		"name": map[string]any{"hasIndex": true, "index": float64(1)},
		"age":  map[string]any{"hasIndex": true, "index": float64(2)},
	}
	s.assertSchemaEquality(expected2, schema)

	resp, err = client.DeleteRule(rule2)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	time.Sleep(100 * time.Millisecond)

	schema, err = client.GetStreamSchema(streamName)
	s.Require().NoError(err)
	s.assertSchemaEquality(expected1, schema)
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
	s.assertSchemaEquality(expected, schema)
}

// A helper struct to hold the nested schema values for sorting and comparison.
type SchemaValue struct {
	HasIndex bool
	Index    float64
}

// assertSchemaEquality compares two schemas for equality, ignoring key-to-index mapping order.
func (s *RuleTestSuite) assertSchemaEquality(expected, actual map[string]any) {
	// 1. Check if the number of keys is the same.
	s.Equal(len(expected), len(actual), "Schema lengths should be equal")

	// 2. Validate that the sets of keys are the same.
	expectedKeys := make([]string, 0, len(expected))
	for k := range expected {
		expectedKeys = append(expectedKeys, k)
	}
	actualKeys := make([]string, 0, len(actual))
	for k := range actual {
		actualKeys = append(actualKeys, k)
	}
	sort.Strings(expectedKeys)
	sort.Strings(actualKeys)
	s.Equal(expectedKeys, actualKeys, "The set of schema keys should be identical")

	// 3. Extract and collect all nested SchemaValue structs from both maps.
	expectedValues := make([]SchemaValue, 0, len(expected))
	actualValues := make([]SchemaValue, 0, len(actual))

	for _, v := range expected {
		nestedMap, ok := v.(map[string]any)
		s.True(ok, "Expected value is not a map[string]any")
		expectedValues = append(expectedValues, SchemaValue{
			HasIndex: nestedMap["hasIndex"].(bool),
			Index:    nestedMap["index"].(float64),
		})
	}

	for _, v := range actual {
		nestedMap, ok := v.(map[string]any)
		s.True(ok, "Actual value is not a map[string]any")
		actualValues = append(actualValues, SchemaValue{
			HasIndex: nestedMap["hasIndex"].(bool),
			Index:    nestedMap["index"].(float64),
		})
	}

	// 4. Sort the slices of SchemaValue structs to create a canonical representation.
	sort.Slice(expectedValues, func(i, j int) bool {
		return expectedValues[i].Index < expectedValues[j].Index
	})
	sort.Slice(actualValues, func(i, j int) bool {
		return actualValues[i].Index < actualValues[j].Index
	})

	// 5. Compare the sorted slices to ensure the nested values are identical.
	s.Equal(expectedValues, actualValues, "The set of nested schema values should be identical")
}

func (s *RuleTestSuite) TestJoinWithLookup() {
	client.DeleteRule("ruleSim11")
	client.DeleteStream("sim11")
	client.DeleteStream("sim12")
	client.DeleteTables("sim13")
	topic := "test11"
	subCh := pubsub.CreateSub(topic, nil, topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, topic)
	data1 := []map[string]any{
		{
			"k": "v1",
		},
	}
	conf1 := map[string]any{
		"data":     data1,
		"interval": "1ms",
		"loop":     false,
	}
	resp1, err := client.CreateConf("sources/simulator/confKeys/sim11", conf1)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp1.StatusCode)
	streamSql1 := `{"sql": "create stream sim11() WITH (TYPE=\"simulator\", CONF_KEY=\"sim11\")"}`
	resp1, err = client.CreateStream(streamSql1)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp1))
	s.Require().Equal(http.StatusCreated, resp1.StatusCode)
	data2 := []map[string]any{
		{
			"k": "v1",
		},
	}
	conf2 := map[string]any{
		"data":     data2,
		"interval": "1ms",
		"loop":     false,
	}
	resp2, err := client.CreateConf("sources/simulator/confKeys/sim12", conf2)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp2.StatusCode)
	streamSql2 := `{"sql": "create stream sim12() WITH (TYPE=\"simulator\", CONF_KEY=\"sim12\")"}`
	resp2, err = client.CreateStream(streamSql2)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp2))
	s.Require().Equal(http.StatusCreated, resp2.StatusCode)

	data3 := []map[string]any{
		{
			"k": "v1",
		},
	}
	conf3 := map[string]any{
		"data": data3,
	}
	resp3, err := client.CreateConf("sources/simulator/confKeys/sim13", conf3)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp3.StatusCode)
	tableSql := `{"sql": "create table sim13() WITH (TYPE=\"simulator\", CONF_KEY=\"sim13\", KIND=\"lookup\")"}`
	resp3, err = client.CreateStream(tableSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp3))
	s.Require().Equal(http.StatusCreated, resp3.StatusCode)

	ruleSql := `{
	   "id": "ruleSim11",
	   "sql": "SELECT sim11.k as k1, sim12.k as k2, sim13.k as k3 from sim11 inner join sim12 on sim11.k = sim12.k inner join sim13 on sim13.k = sim11.k group by countwindow(2)",
	   "actions": [
	       {
	           "memory": {
	               "topic": "test11"
	           }
	       }
	   ],
	   "options": {
	
	   }
	}`
	resp, err := client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	s.assertRecvMemTuple(subCh, []map[string]any{{"k1": "v1", "k2": "v1", "k3": "v1"}})
}

// TestDataTemplateArrayDecode tests that dataTemplate producing a JSON array `[{},{}]`
// is correctly decoded by sink_node and produces a list for memory sink/source
func (s *RuleTestSuite) TestDataTemplateArrayDecode() {
	client.DeleteRule("ruleArrayDecode")
	client.DeleteStream("simArray")
	defer client.DeleteRule("ruleArrayDecode")
	defer client.DeleteStream("simArray")

	topic := "arrayDecodeResult"
	subCh := pubsub.CreateSub(topic, nil, topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, topic)

	// Configure simulator source with single input that will be expanded to array
	data := []map[string]any{
		{"a": float64(1), "b": float64(2)},
	}
	conf := map[string]any{
		"data":     data,
		"interval": "10ms",
		"loop":     false,
	}
	resp, err := client.CreateConf("sources/simulator/confKeys/simArray", conf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	streamSql := `{"sql": "create stream simArray() WITH (TYPE=\"simulator\", CONF_KEY=\"simArray\")"}`
	resp, err = client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// Create rule with dataTemplate that outputs JSON array
	// The sink_node should decode this and call CollectList
	ruleSql := `{
		"id": "ruleArrayDecode",
		"sql": "SELECT * FROM simArray",
		"actions": [
			{
				"memory": {
					"topic": "arrayDecodeResult",
					"format": "json",
					"dataTemplate": "[{\"v\":{{.a}}},{\"v\":{{.b}}}]",
					"sendSingle": true
				}
			}
		]
	}`
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// When the decoder returns a list, memory source should receive a list
	expected := []map[string]any{
		{"v": float64(1)},
		{"v": float64(2)},
	}
	s.assertRecvMemTupleList(subCh, expected)
}

// TestBatchDataTemplateJSON verifies the complete bytes-sink pipeline:
// Batch -> Transform (RawTuple) -> JSON Writer -> REST.
func (s *RuleTestSuite) TestBatchDataTemplateJSON() {
	const (
		ruleID     = "ruleBatchDataTemplate"
		streamName = "simBatchDataTemplate"
	)
	client.DeleteRule(ruleID)
	client.DeleteStream(streamName)
	defer client.DeleteRule(ruleID)
	defer client.DeleteStream(streamName)
	privateNetEnabled := conf.Config.Basic.EnablePrivateNet
	conf.Config.Basic.EnablePrivateNet = true
	defer func() {
		conf.Config.Basic.EnablePrivateNet = privateNetEnabled
	}()

	received := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := map[string]any{
		"data": []map[string]any{
			{"id": float64(1), "temperature": float64(20)},
			{"id": float64(2), "temperature": float64(30)},
		},
		"interval": "10ms",
		"loop":     false,
	}
	resp, err := client.CreateConf("sources/simulator/confKeys/"+streamName, conf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	streamSQL := fmt.Sprintf(`{"sql":"CREATE STREAM %s() WITH (TYPE=\"simulator\", CONF_KEY=\"%s\")"}`, streamName, streamName)
	resp, err = client.CreateStream(streamSQL)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	ruleSQL := fmt.Sprintf(`{
		"id": %q,
		"sql": "SELECT * FROM %s",
		"actions": [{
			"rest": {
				"url": %q,
				"method": "POST",
				"bodyType": "json",
				"format": "json",
				"batchSize": 2,
				"sendSingle": true,
				"dataTemplate": "{\"deviceId\":{{.id}},\"value\":{{.temperature}}}"
			}
		}]
	}`, ruleID, streamName, server.URL)
	resp, err = client.CreateRule(ruleSQL)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	select {
	case body := <-received:
		var actual []map[string]any
		s.Require().NoError(json.Unmarshal(body, &actual), "REST body: %s", body)
		s.Require().Equal([]map[string]any{
			{"deviceId": float64(1), "value": float64(20)},
			{"deviceId": float64(2), "value": float64(30)},
		}, actual)
	case <-time.After(5 * time.Second):
		s.Fail("timed out waiting for batched REST request")
	}
}

func (s *RuleTestSuite) TestRestSinkOAuthAndRowHeaderTemplates() {
	const (
		streamID = "restOAuthHeaderStream"
		ruleID   = "restOAuthHeaderRule"
		confKey  = "restOAuthHeaderSource"
	)
	client.DeleteRule(ruleID)
	client.DeleteStream(streamID)
	defer client.DeleteRule(ruleID)
	defer client.DeleteStream(streamID)
	originalEnablePrivateNet := conf.Config.Basic.EnablePrivateNet
	conf.Config.Basic.EnablePrivateNet = true
	defer func() {
		conf.Config.Basic.EnablePrivateNet = originalEnablePrivateNet
	}()

	type receivedRequest struct {
		authorization string
		rowHeader     string
	}
	received := make(chan receivedRequest, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "e2e-token",
				"expires_in":   3600,
			})
		case "/target":
			request := receivedRequest{
				authorization: r.Header.Get("Authorization"),
				rowHeader:     r.Header.Get("X-Row"),
			}
			select {
			case received <- request:
			default:
			}
			if request.authorization != "Bearer e2e-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	resp, err := client.CreateConf("sources/simulator/confKeys/"+confKey, map[string]any{
		"data": []map[string]any{
			{"row_value": "row-value", "number": float64(42)},
		},
		"interval": "10ms",
		"loop":     false,
	})
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	streamSQL, err := json.Marshal(map[string]string{
		"sql": fmt.Sprintf(`CREATE STREAM %s() WITH (TYPE="simulator", CONF_KEY="%s")`, streamID, confKey),
	})
	s.Require().NoError(err)
	resp, err = client.CreateStream(string(streamSQL))
	s.Require().NoError(err)
	streamResponse, err := GetResponseText(resp)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode, streamResponse)

	rule, err := json.Marshal(map[string]any{
		"id":  ruleID,
		"sql": fmt.Sprintf("SELECT row_value, number FROM %s", streamID),
		"actions": []map[string]any{
			{
				"rest": map[string]any{
					"url":    ts.URL + "/target",
					"method": http.MethodPost,
					"headers": map[string]string{
						"Authorization": "Bearer {{.access_token}}",
						"Content-Type":  "application/json",
						"X-Row":         `{{index . 0 "row_value"}}`,
					},
					"oauth": map[string]any{
						"access": map[string]any{
							"url":    ts.URL + "/token",
							"body":   `{"grant_type":"client_credentials"}`,
							"expire": "3600",
						},
					},
				},
			},
		},
	})
	s.Require().NoError(err)
	resp, err = client.CreateRule(string(rule))
	s.Require().NoError(err)
	ruleResponse, err := GetResponseText(resp)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode, ruleResponse)

	select {
	case request := <-received:
		s.Require().Equal("Bearer e2e-token", request.authorization)
		s.Require().Equal("row-value", request.rowHeader)
	case <-time.After(10 * time.Second):
		s.Fail("timed out waiting for the first REST request")
	}
}

func (s *RuleTestSuite) TestShowScanTableContentWithErrors() {
	s.Run("testNoJoin", func() {
		client.DeleteStream("simpleStream")
		client.DeleteRule("testRule")
		defer client.DeleteStream("simpleStream")
		dataLoc, err := conf.GetDataLoc()
		s.Require().NoError(err)

		devicesData := []map[string]interface{}{
			{"id": int64(1), "name": "Device1", "location": "Room1"},
			{"id": int64(2), "name": "Device2", "location": "Room2"},
			{"id": int64(3), "name": "Device3", "location": "Room3"},
		}

		devicesJSON, marshalErr := json.Marshal(devicesData)
		s.Require().NoError(marshalErr)
		devicesPath := filepath.Join(dataLoc, "devices.json")
		err = os.WriteFile(devicesPath, devicesJSON, 0o644)
		s.Require().NoError(err)
		defer os.Remove(devicesPath)

		streamSql := `{"sql": "CREATE STREAM simpleStream (id BIGINT, temperature FLOAT, humidity FLOAT) WITH (DATASOURCE=\"sensorStream\", TYPE=\"simulator\", FORMAT=\"json\")"}`

		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `
    	{
        	"id": "testRule",
        	"sql": "SELECT count(*) FROM simpleStream",
        	"actions": [
        	{
            	"log": {}
        	}
        	]
    	}
    	`
		defer client.DeleteRule("testRule")

		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		require.Eventually(s.T(), func() bool {
			resp, err := client.GetRuleScanTablesSnapshot("testRule")
			if err != nil {
				return false
			}

			s.T().Log(GetResponseText(resp))
			return http.StatusBadRequest == resp.StatusCode
		}, 3*time.Second, 200*time.Millisecond)
	})

	s.Run("testNoData", func() {
		client.DeleteTables("deviceTable")
		client.DeleteRule("joinTestRule")
		client.DeleteStream("sensorStream")
		defer client.DeleteStream("sensorStream")
		defer client.DeleteTables("deviceTable")

		dataLoc, err := conf.GetDataLoc()
		s.Require().NoError(err)
		devicesPath := filepath.Join(dataLoc, "devices.json")
		err = os.WriteFile(devicesPath, []byte(``), 0o644)
		s.Require().NoError(err)
		defer os.Remove(devicesPath)

		streamSql := `{"sql": "CREATE STREAM sensorStream (id BIGINT, temperature FLOAT, humidity FLOAT) WITH (DATASOURCE=\"sensorStream\", TYPE=\"simulator\", FORMAT=\"json\")"}`

		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		tableSql := `{"sql": "CREATE TABLE deviceTable (id BIGINT, name STRING, location STRING) WITH (DATASOURCE=\"devices.json\", FORMAT=\"json\", TYPE=\"file\")"}`

		resp, err = client.CreateTable(tableSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `
    	{
        	"id": "joinTestRule",
        	"sql": "SELECT sensorStream.id, sensorStream.temperature, deviceTable.name, deviceTable.location FROM sensorStream INNER JOIN deviceTable ON sensorStream.id = deviceTable.id",
        	"actions": [
        	{
            	"log": {}
        	}
        	]
    	}
    	`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		require.Eventually(s.T(), func() bool {
			resp, err := client.GetRuleScanTablesSnapshot("joinTestRule")
			if err != nil {
				return false
			}
			s.T().Log(GetResponseText(resp))

			return http.StatusBadRequest == resp.StatusCode
		}, 3*time.Second, 200*time.Millisecond)
	})
}

func (s *RuleTestSuite) TestShowScanTableContent() {
	client.DeleteStream("sensorStream")
	client.DeleteTables("deviceTable")
	client.DeleteRule("joinTestRule")

	defer client.DeleteStream("sensorStream")
	defer client.DeleteTables("deviceTable")
	defer client.DeleteRule("joinTestRule")

	dataLoc, err := conf.GetDataLoc()
	s.Require().NoError(err)

	devicesData := []map[string]interface{}{
		{"id": int64(1), "name": "Device1", "location": "Room1"},
		{"id": int64(2), "name": "Device2", "location": "Room2"},
		{"id": int64(3), "name": "Device3", "location": "Room3"},
	}
	devicesJSON, marshalErr := json.Marshal(devicesData)
	s.Require().NoError(marshalErr)
	devicesPath := filepath.Join(dataLoc, "devices.json")
	err = os.WriteFile(devicesPath, devicesJSON, 0o644)
	s.Require().NoError(err)
	defer os.Remove(devicesPath)

	streamSql := `{"sql": "CREATE STREAM sensorStream (id BIGINT, temperature FLOAT, humidity FLOAT) WITH (DATASOURCE=\"sensorStream\", TYPE=\"simulator\", FORMAT=\"json\")"}`

	resp, err := client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	tableSql := `{"sql": "CREATE TABLE deviceTable (id BIGINT, name STRING, location STRING) WITH (DATASOURCE=\"devices.json\", FORMAT=\"json\", TYPE=\"file\")"}`

	resp, err = client.CreateTable(tableSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	ruleSql := `
    {
        "id": "joinTestRule",
        "sql": "SELECT sensorStream.id, sensorStream.temperature, deviceTable.name, deviceTable.location FROM sensorStream INNER JOIN deviceTable ON sensorStream.id = deviceTable.id",
        "actions": [
        {
            "log": {}
        }
        ]
    }
    `
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	var result []server.ShowScanTablesResponse
	require.Eventually(s.T(), func() bool {
		resp, err = client.GetRuleScanTablesSnapshot("joinTestRule")
		if err != nil {
			return false
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return false
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return false
		}

		var r []server.ShowScanTablesResponse
		if unmarshalErr := json.Unmarshal(body, &r); unmarshalErr != nil {
			return false
		}

		if len(r) < 3 {
			return false
		}

		result = r
		return true
	}, 5*time.Second, 200*time.Millisecond)

	expectedDevices := []map[string]interface{}{
		{"id": float64(1), "name": "Device1", "location": "Room1"},
		{"id": float64(2), "name": "Device2", "location": "Room2"},
		{"id": float64(3), "name": "Device3", "location": "Room3"},
	}

	s.Require().GreaterOrEqual(len(result), len(expectedDevices), "Mismatch between result and expected result")

	for i, expected := range expectedDevices {
		s.Require().Equal("deviceTable", result[i].Emitter)
		s.Require().Equal(expected, result[i].ScanTableContent,
			fmt.Sprintf("Tuple %d content mismatch", i))
	}
}

func (s *RuleTestSuite) assertRecvMemTupleList(subCh chan any, expect []map[string]any) {
	d := <-subCh
	mt, ok := d.([]pubsub.MemTuple)
	s.Require().True(ok, "expected []pubsub.MemTuple but got %T", d)
	s.Require().Len(mt, len(expect))
	for i, e := range expect {
		s.Require().Equal(e, mt[i].ToMap())
	}
}
