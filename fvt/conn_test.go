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
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
)

type ConnectionTestSuite struct {
	suite.Suite
}

func TestConnectionTestSuite(t *testing.T) {
	suite.Run(t, new(ConnectionTestSuite))
}

func (s *ConnectionTestSuite) TestConnStatus() {
	// Connect when broker is not started
	s.Run("create rule when broker is not started", func() {
		// create connection
		connStr := `{
			"id": "conn1",
			"typ":"mqtt",
			"props": {
				"server": "tcp://127.0.0.1:3883"
            }
		}`
		resp, err := client.Post("connections", connStr)
		s.Require().NoError(err)
		fmt.Println(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		conf := map[string]any{
			"connectionSelector": "conn1",
		}
		resp, err = client.CreateConf("sources/mqtt/confKeys/ttt", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql": "create stream tttStream () WITH (TYPE=\"mqtt\", DATASOURCE=\"ttt\", FORMAT=\"json\", CONF_KEY=\"ttt\", SHARED=\"true\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
		  "id": "ruleTTT1",
		  "sql": "SELECT * FROM tttStream",
		  "actions": [
			{
			  "nop": {
			  }
			}
		  ]
		}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		// Assert connection status
		r := TryAssert(10, ConstantInterval, func() bool {
			get, e := client.Get("connections/conn1")
			s.Require().NoError(e)
			resultMap, e := GetResponseResultMap(get)
			fmt.Println(resultMap)
			s.Require().NoError(e)
			return resultMap["status"] == "disconnected"
		})
		s.Require().True(r)
		// Assert rule metrics
		r = TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT1")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == -1.0
		})
		s.Require().True(r)
	})
	var (
		server *mqtt.Server
		tcp    *listeners.TCP
	)
	s.Run("start broker, automatically connected", func() {
		// Create the new MQTT Server.
		server = mqtt.New(nil)
		// Allow all connections.
		_ = server.AddHook(new(auth.AllowHook), nil)
		// Create a TCP listener on a standard port.
		tcp = listeners.NewTCP(listeners.Config{ID: "testcon", Address: ":3883"})
		err := server.AddListener(tcp)
		s.Require().NoError(err)
		go func() {
			err = server.Serve()
			fmt.Println(err)
		}()
		fmt.Println(tcp.Address())
		// Assert rule metrics
		r := TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT1")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == 1.0
		})
		s.Require().True(r)
		// Assert connection status
		r = TryAssert(10, ConstantInterval, func() bool {
			get, e := client.Get("connections/conn1")
			s.Require().NoError(e)
			resultMap, e := GetResponseResultMap(get)
			fmt.Println(resultMap)
			s.Require().NoError(e)
			return resultMap["status"] == "connected"
		})
		s.Require().True(r)
	})
	s.Run("attach rule, get status", func() {
		ruleSql := `{
		  "id": "ruleTTT2",
		  "sql": "SELECT * FROM tttStream",
		  "actions": [
			{
			  "mqtt": {
				"connectionSelector": "conn1",
				"topic":"result"
			  }
			}
		  ]
		}`
		resp, err := client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		// Assert rule2 metrics
		r := TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT2")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == 1.0
		})
		s.Require().True(r)
	})
	s.Run("stop broker, check status update", func() {
		err := server.Close()
		tcp.Close(nil)
		s.Require().NoError(err)
		// Assert rule1 metrics
		r := TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT1")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == 0.0
		})
		s.Require().True(r)
		// Assert rule1 metrics
		r = TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT2")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == 0.0
		})
		s.Require().True(r)
		// Assert connection status
		r = TryAssert(10, ConstantInterval, func() bool {
			get, e := client.Get("connections/conn1")
			s.Require().NoError(e)
			resultMap, e := GetResponseResultMap(get)
			fmt.Println(resultMap)
			s.Require().NoError(e)
			return resultMap["status"] == "connecting"
		})
		s.Require().True(r)
	})
	s.Run("restart broker, check status update", func() {
		// Create the new MQTT Server.
		server = mqtt.New(nil)
		// Allow all connections.
		_ = server.AddHook(new(auth.AllowHook), nil)
		// Create a TCP listener on a standard port.
		tcp = listeners.NewTCP(listeners.Config{ID: "testcon", Address: ":3883"})
		err := server.AddListener(tcp)
		s.Require().NoError(err)
		go func() {
			err = server.Serve()
			fmt.Println(err)
		}()
		fmt.Println(tcp.Address())
		// Assert rule2 metrics
		r := TryAssert(10, time.Second, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT2")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == 1.0
		})
		s.Require().True(r)
		// Assert connection status
		r = TryAssert(10, time.Second, func() bool {
			get, e := client.Get("connections/conn1")
			s.Require().NoError(e)
			resultMap, e := GetResponseResultMap(get)
			fmt.Println(resultMap)
			s.Require().NoError(e)
			return resultMap["status"] == "connected"
		})
		s.Require().True(r)
		// Assert rule1 metrics
		r = TryAssert(10, time.Second, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT1")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_conn1/ttt_0_connection_status"] == 1.0
		})
		s.Require().True(r)
	})
	s.Run("clean", func() {
		res, e := client.Delete("rules/ruleTTT1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/ruleTTT2")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/tttStream")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		r := TryAssert(10, ConstantInterval, func() bool {
			res, e = client.Delete("connections/conn1")
			s.NoError(e)
			return res.StatusCode == http.StatusOK
		})
		s.Require().True(r)
	})
}

func (s *ConnectionTestSuite) TestSourcePing() {
	tests := []struct {
		name    string
		props   map[string]any
		timeout bool
		err     string
	}{
		{
			name: "mqtt",
			props: map[string]any{
				"server": "tcp://127.0.0.1:1883",
			},
			err: "{\"error\":1003,\"message\":\"found error when connecting for tcp://127.0.0.1:1883: network Error : dial tcp 127.0.0.1:1883: connect: connection refused\"}\n",
		},
		{
			name: "httppull",
			props: map[string]any{
				"url": "https://www.githubstatus.com/api/v2/status.json",
			},
			err: "{\"error\":1000,\"message\":\"source httppull doesn't support ping connection\"}\n",
		},
		{
			name:  "httppush",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"source httppush doesn't support ping connection\"}\n",
		},
		{
			name: "neuron",
			props: map[string]any{
				"url": "tcp://127.0.0.1:7081",
			},
			err: "{\"error\":1000,\"message\":\"source neuron doesn't support ping connection\"}\n",
		},
		{
			name:  "file",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"source file doesn't support ping connection\"}\n",
		},
		{
			name:  "memory",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"source memory doesn't support ping connection\"}\n",
		},
		{
			name:  "websocket",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"source websocket doesn't support ping connection\"}\n",
		},
		{
			name:  "simulator",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"source simulator doesn't support ping connection\"}\n",
		},
		{
			name:  "video",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"source video doesn't support ping connection\"}\n",
		},
		{
			name: "kafka",
			props: map[string]any{
				"brokers": "tcp://127.0.0.1:1883",
				"topic":   "test",
			},
			err: "{\"error\":1000,\"message\":\"failed to dial: failed to open connection to [tcp://127.0.0.1:1883]:9092: dial tcp: lookup tcp://127.0.0.1:1883: no such host\"}\n",
		},
		//{
		//	name: "sql",
		//	props: map[string]any{
		//		"url": "mysql://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
		//	},
		//	timeout: true,
		//	err:     "{\"error\":1000,\"message\":\"dial tcp 127.0.0.1:3306: connectex: No connection could be made because the target machine actively refused it.\"}\n",
		//},
	}
	prefix := "metadata/sources/connection"
	for _, tt := range tests {
		s.Run("ping source "+tt.name, func() {
			body, err := json.Marshal(tt.props)
			s.Require().NoError(err)
			resp, err := client.Post(path.Join(prefix, tt.name), string(body))
			if tt.timeout {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
				if tt.err == "" {
					s.Require().Equal(http.StatusOK, resp.StatusCode)
				} else {
					s.Require().Equal(http.StatusBadRequest, resp.StatusCode)
					t, err := GetResponseText(resp)
					s.Require().NoError(err)
					s.Require().Equal(tt.err, t)
				}
			}
		})
	}
}

func (s *ConnectionTestSuite) TestLookupSourcePing() {
	tests := []struct {
		name    string
		props   map[string]any
		timeout bool
		err     string
	}{
		{
			name: "httppull",
			props: map[string]any{
				"url": "https://www.githubstatus.com/api/v2/status.json",
			},
			err: "{\"error\":1000,\"message\":\"lookup source httppull doesn't support ping connection\"}\n",
		},
		{
			name:  "memory",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"lookup source memory doesn't support ping connection\"}\n",
		},
		//{
		//	name: "sql",
		//	props: map[string]any{
		//		"url": "mysql://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
		//	},
		//	timeout: true,
		//	err:     "{\"error\":1000,\"message\":\"dial tcp 127.0.0.1:3306: connectex: No connection could be made because the target machine actively refused it.\"}\n",
		//},
	}
	prefix := "metadata/lookups/connection"
	for _, tt := range tests {
		s.Run("ping source "+tt.name, func() {
			body, err := json.Marshal(tt.props)
			s.Require().NoError(err)
			resp, err := client.Post(path.Join(prefix, tt.name), string(body))
			if tt.timeout {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
				if tt.err == "" {
					s.Require().Equal(http.StatusOK, resp.StatusCode)
				} else {
					s.Require().Equal(http.StatusBadRequest, resp.StatusCode)
					t, err := GetResponseText(resp)
					s.Require().NoError(err)
					s.Require().Equal(tt.err, t)
				}
			}
		})
	}
}

func (s *ConnectionTestSuite) TestSinkPing() {
	tests := []struct {
		name    string
		props   map[string]any
		timeout bool
		err     string
	}{
		{
			name: "mqtt",
			props: map[string]any{
				"server": "tcp://127.0.0.1:1883",
			},
			err: "{\"error\":1003,\"message\":\"found error when connecting for tcp://127.0.0.1:1883: network Error : dial tcp 127.0.0.1:1883: connect: connection refused\"}\n",
		},
		{
			name: "rest",
			props: map[string]any{
				"url": "https://www.githubstatus.com/api/v2/status.json",
			},
			err: "{\"error\":1000,\"message\":\"sink rest doesn't support ping connection\"}\n",
		},
		{
			name: "neuron",
			props: map[string]any{
				"url": "tcp://127.0.0.1:7081",
			},
			err: "{\"error\":1000,\"message\":\"sink neuron doesn't support ping connection\"}\n",
		},
		{
			name:  "file",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"sink file doesn't support ping connection\"}\n",
		},
		{
			name:  "memory",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"sink memory doesn't support ping connection\"}\n",
		},
		{
			name:  "websocket",
			props: map[string]any{},
			err:   "{\"error\":1000,\"message\":\"sink websocket doesn't support ping connection\"}\n",
		},
		{
			name: "kafka",
			props: map[string]any{
				"brokers": "tcp://127.0.0.1:1883",
				"topic":   "test",
			},
			err: "{\"error\":1000,\"message\":\"failed to dial: failed to open connection to [tcp://127.0.0.1:1883]:9092: dial tcp: lookup tcp://127.0.0.1:1883: no such host\"}\n",
		},
		//{
		//	name: "sql",
		//	props: map[string]any{
		//		"url": "mysql://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
		//	},
		//	timeout: true,
		//	// err: "{\"error\":1000,\"message\":\"dial tcp 127.0.0.1:3306: connectex: No connection could be made because the target machine actively refused it.\"}\n",
		//},
		{
			name: "influx",
			props: map[string]any{
				"addr":        "http://test.com/test?parseTime=true",
				"database":    "test",
				"measurement": "test",
			},
			timeout: true,
			err:     "\"error\":1000",
		},
		{
			name: "influx2",
			props: map[string]any{
				"addr":        "http://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
				"database":    "test",
				"org":         "test",
				"bucket":      "test",
				"measurement": "test",
			},
			timeout: true,
			err:     "error connecting to influxdb2",
		},
	}
	prefix := "metadata/sinks/connection"
	for _, tt := range tests {
		s.Run("ping sink "+tt.name, func() {
			body, err := json.Marshal(tt.props)
			s.Require().NoError(err)
			resp, err := client.Post(path.Join(prefix, tt.name), string(body))
			if tt.timeout {
				if err != nil {
					s.Require().Error(err)
					return
				}
			}
			s.Require().NoError(err)
			if tt.err == "" {
				s.Require().Equal(http.StatusOK, resp.StatusCode)
			} else {
				s.Require().Equal(http.StatusBadRequest, resp.StatusCode)
				t, err := GetResponseText(resp)
				s.Require().NoError(err)
				if !strings.Contains(t, tt.err) {
					s.Require().Equal(tt.err, t)
				}
			}
		})
	}
}

// TestSharedConnE2E validates that a single named MQTT connection can back a shared stream
// and multiple non-shared streams simultaneously, with each rule receiving only the data
// published to its respective MQTT topic.
func (s *ConnectionTestSuite) TestSharedConnE2E() {
	// Start a local mock MQTT broker for this test.
	mockBroker := mqtt.New(nil)
	_ = mockBroker.AddHook(new(auth.AllowHook), nil)
	mockTCP := listeners.NewTCP(listeners.Config{ID: "sharedE2EBroker", Address: ":1884"})
	s.Require().NoError(mockBroker.AddListener(mockTCP))
	go func() { _ = mockBroker.Serve() }()
	defer mockBroker.Close()
	const localBroker = "tcp://127.0.0.1:1884"

	// Pre-clean any leftover state from a previous interrupted run.
	client.Delete("rules/sharedE2ERule1")
	client.Delete("rules/sharedE2ERule2")
	client.Delete("rules/sharedE2ERule3")
	client.Delete("rules/sharedE2ERule4")
	client.Delete("rules/sharedE2ERule5")
	client.Delete("streams/sharedE2EStream1")
	client.Delete("streams/sharedE2EStream2")
	client.Delete("streams/sharedE2EStream3")
	client.Delete("connections/sharedE2EConn")

	// Create result channels before rules so the memory sink can deliver into them.
	const (
		result1Topic = "shared_e2e_result1"
		result2Topic = "shared_e2e_result2"
		result3Topic = "shared_e2e_result3"
		result4Topic = "shared_e2e_result4"
		result5Topic = "shared_e2e_result5"
	)
	subCh1 := pubsub.CreateSub(result1Topic, nil, result1Topic, 1024)
	subCh2 := pubsub.CreateSub(result2Topic, nil, result2Topic, 1024)
	subCh3 := pubsub.CreateSub(result3Topic, nil, result3Topic, 1024)
	subCh4 := pubsub.CreateSub(result4Topic, nil, result4Topic, 1024)
	subCh5 := pubsub.CreateSub(result5Topic, nil, result5Topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(result1Topic, result1Topic)
	defer pubsub.CloseSourceConsumerChannel(result2Topic, result2Topic)
	defer pubsub.CloseSourceConsumerChannel(result3Topic, result3Topic)
	defer pubsub.CloseSourceConsumerChannel(result4Topic, result4Topic)
	defer pubsub.CloseSourceConsumerChannel(result5Topic, result5Topic)

	// Connect a paho publisher to the local mock broker.
	pahoOpts := pahomqtt.NewClientOptions().
		AddBroker(localBroker).
		SetClientID("sharedE2EPublisher")
	publisher := pahomqtt.NewClient(pahoOpts)
	token := publisher.Connect()
	token.Wait()
	s.Require().NoError(token.Error())
	defer publisher.Disconnect(200)

	s.Run("create connection", func() {
		connStr := fmt.Sprintf(`{
			"id": "sharedE2EConn",
			"typ": "mqtt",
			"props": {
				"server": "%s"
			}
		}`, localBroker)
		resp, err := client.Post("connections", connStr)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})

	s.Run("create streams", func() {
		// Each conf key points to the shared connection; the DATASOURCE in the stream SQL
		// determines which MQTT topic the stream subscribes to.
		for _, confKey := range []string{"sharedE2EConf1", "sharedE2EConf2", "sharedE2EConf3"} {
			conf := map[string]any{"connectionSelector": "sharedE2EConn"}
			resp, err := client.CreateConf("sources/mqtt/confKeys/"+confKey, conf)
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
		}

		// shared1 — SHARED="true" so multiple rules reuse a single subscriber.
		resp, err := client.CreateStream(`{"sql": "create stream sharedE2EStream1() WITH (TYPE=\"mqtt\", DATASOURCE=\"shared_e2e/shared\", FORMAT=\"json\", CONF_KEY=\"sharedE2EConf1\", SHARED=\"true\")"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		// stream2 — non-shared, subscribes to its own topic.
		resp, err = client.CreateStream(`{"sql": "create stream sharedE2EStream2() WITH (TYPE=\"mqtt\", DATASOURCE=\"shared_e2e/stream2\", FORMAT=\"json\", CONF_KEY=\"sharedE2EConf2\")"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		// stream3 — SHARED="true" so rule4 and rule5 both receive each message.
		resp, err = client.CreateStream(`{"sql": "create stream sharedE2EStream3() WITH (TYPE=\"mqtt\", DATASOURCE=\"shared_e2e/stream3\", FORMAT=\"json\", CONF_KEY=\"sharedE2EConf3\", SHARED=\"true\")"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})

	s.Run("create rules", func() {
		// Two rules on the shared stream — both receive every message on shared_e2e/shared.
		for _, r := range []struct {
			id     string
			result string
		}{
			{"sharedE2ERule1", result1Topic},
			{"sharedE2ERule2", result2Topic},
		} {
			ruleSql := fmt.Sprintf(`{
				"id": "%s",
				"sql": "SELECT * FROM sharedE2EStream1",
				"actions": [{"memory": {"topic": "%s"}}]
			}`, r.id, r.result)
			resp, err := client.CreateRule(ruleSql)
			s.Require().NoError(err)
			s.T().Log(GetResponseText(resp))
			s.Require().Equal(http.StatusCreated, resp.StatusCode)
		}

		// One rule on stream2.
		resp, err := client.CreateRule(fmt.Sprintf(`{
			"id": "sharedE2ERule3",
			"sql": "SELECT * FROM sharedE2EStream2",
			"actions": [{"memory": {"topic": "%s"}}]
		}`, result3Topic))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		// Two rules on stream3 — both receive every message because stream3 is shared.
		for _, r := range []struct {
			id     string
			result string
		}{
			{"sharedE2ERule4", result4Topic},
			{"sharedE2ERule5", result5Topic},
		} {
			resp, err = client.CreateRule(fmt.Sprintf(`{
				"id": "%s",
				"sql": "SELECT * FROM sharedE2EStream3",
				"actions": [{"memory": {"topic": "%s"}}]
			}`, r.id, r.result))
			s.Require().NoError(err)
			s.Require().Equal(http.StatusCreated, resp.StatusCode)
		}
	})

	s.Run("wait for connection established", func() {
		r := TryAssert(20, ConstantInterval, func() bool {
			get, e := client.Get("connections/sharedE2EConn")
			s.Require().NoError(e)
			resultMap, e := GetResponseResultMap(get)
			s.Require().NoError(e)
			return resultMap["status"] == "connected"
		})
		s.Require().True(r, "shared connection did not reach 'connected' state in time")
	})

	s.Run("publish to shared stream and assert both rules receive", func() {
		for i := range 100 {
			token := publisher.Publish("shared_e2e/shared", 0, false, fmt.Sprintf(`{"v":%d}`, i))
			token.Wait()
			s.Require().NoError(token.Error())
			time.Sleep(time.Millisecond)
		}
		for i := range 100 {
			s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(i)})
			s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(i)})
		}
	})

	s.Run("publish to stream2 and assert only rule3 receives", func() {
		for i := range 100 {
			token := publisher.Publish("shared_e2e/stream2", 0, false, fmt.Sprintf(`{"v":%d}`, i))
			token.Wait()
			s.Require().NoError(token.Error())
			time.Sleep(time.Millisecond)
		}
		for i := range 100 {
			s.assertConnRecvMemTuple(subCh3, map[string]any{"v": float64(i)})
		}
	})

	s.Run("publish to stream3 and assert both rule4 and rule5 receive", func() {
		for i := range 100 {
			token := publisher.Publish("shared_e2e/stream3", 0, false, fmt.Sprintf(`{"v":%d}`, i))
			token.Wait()
			s.Require().NoError(token.Error())
			time.Sleep(time.Millisecond)
		}
		for i := range 100 {
			s.assertConnRecvMemTuple(subCh4, map[string]any{"v": float64(i)})
			s.assertConnRecvMemTuple(subCh5, map[string]any{"v": float64(i)})
		}
	})

	s.Run("clean", func() {
		for _, rule := range []string{"sharedE2ERule1", "sharedE2ERule2", "sharedE2ERule3", "sharedE2ERule4", "sharedE2ERule5"} {
			res, e := client.Delete("rules/" + rule)
			s.NoError(e)
			s.Equal(http.StatusOK, res.StatusCode)
		}
		for _, stream := range []string{"sharedE2EStream1", "sharedE2EStream2", "sharedE2EStream3"} {
			res, e := client.Delete("streams/" + stream)
			s.NoError(e)
			s.Equal(http.StatusOK, res.StatusCode)
		}
		r := TryAssert(10, ConstantInterval, func() bool {
			res, e := client.Delete("connections/sharedE2EConn")
			s.NoError(e)
			return res.StatusCode == http.StatusOK
		})
		s.Require().True(r, "could not delete shared connection")
	})
}

// TestSharedConnStaleSubscription tests that a non-shared stream backed by a named MQTT
// connection remains live after various rule restart patterns.
//
// Customer symptom: a rule shows status "running" and connection_status "connected" but
// records_in_total and last_invocation stop advancing, without that rule ever being
// restarted by the user. Only OTHER rules on the same connection were restarted.
//
// Root cause (Scenario 2 from SubTopo lifecycle analysis):
//   - Two rules (A and B) share the same SrcSubTopo (same connectionSelector + datasource).
//   - Both rules are stopped, evicting the SubTopo from the pool (opened → CloseState).
//   - Both rules are started concurrently. One rule's planning call to GetOrCreateSubTopo
//     sees the pool entry before CloseSubTopo deletes it, so updateRef finds CloseState
//     and returns false — that rule's ref is silently not registered.
//   - CloseSubTopo then destroys the SubTopo. The other rule is connected to a dead source,
//     never receives data, yet reports as running with frozen metrics.
//
// The sub-tests below are end-to-end regression coverage: without test-hook control over
// SubTopo timing the race is probabilistic, but each pattern raises the likelihood that a
// latent reference-counting gap will be exposed.
func (s *ConnectionTestSuite) TestSharedConnStaleSubscription() {
	// Start a local mock MQTT broker for this test.
	mockBroker := mqtt.New(nil)
	_ = mockBroker.AddHook(new(auth.AllowHook), nil)
	mockTCP := listeners.NewTCP(listeners.Config{ID: "staleBroker", Address: ":1885"})
	s.Require().NoError(mockBroker.AddListener(mockTCP))
	go func() { _ = mockBroker.Serve() }()
	defer mockBroker.Close()
	const localBroker = "tcp://127.0.0.1:1885"

	client.Delete("rules/staleRule1")
	client.Delete("rules/staleRule2")
	client.Delete("streams/staleStream")
	client.Delete("connections/staleConn")

	const (
		staleTopic1 = "stale_sub_result1"
		staleTopic2 = "stale_sub_result2"
	)
	subCh1 := pubsub.CreateSub(staleTopic1, nil, staleTopic1, 1024)
	subCh2 := pubsub.CreateSub(staleTopic2, nil, staleTopic2, 1024)
	defer pubsub.CloseSourceConsumerChannel(staleTopic1, staleTopic1)
	defer pubsub.CloseSourceConsumerChannel(staleTopic2, staleTopic2)

	pahoOpts := pahomqtt.NewClientOptions().
		AddBroker(localBroker).
		SetClientID("staleConnPublisher")
	publisher := pahomqtt.NewClient(pahoOpts)
	tok := publisher.Connect()
	tok.Wait()
	s.Require().NoError(tok.Error())
	defer publisher.Disconnect(200)

	pub := func(val int) {
		t := publisher.Publish("stale/data", 0, false, fmt.Sprintf(`{"v":%d}`, val))
		t.Wait()
		s.Require().NoError(t.Error())
		time.Sleep(time.Millisecond)
	}

	s.Run("setup", func() {
		resp, err := client.Post("connections", fmt.Sprintf(`{"id":"staleConn","typ":"mqtt","props":{"server":"%s"}}`, localBroker))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		conf := map[string]any{"connectionSelector": "staleConn"}
		resp, err = client.CreateConf("sources/mqtt/confKeys/staleConf", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		// Non-shared stream: two rules using it share the same SubTopo (same conn + datasource).
		resp, err = client.CreateStream(`{"sql":"create stream staleStream() WITH (TYPE=\"mqtt\",DATASOURCE=\"stale/data\",FORMAT=\"json\",CONF_KEY=\"staleConf\")"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		resp, err = client.CreateRule(fmt.Sprintf(`{"id":"staleRule1","sql":"SELECT * FROM staleStream","actions":[{"memory":{"topic":"%s"}}]}`, staleTopic1))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		resp, err = client.CreateRule(fmt.Sprintf(`{"id":"staleRule2","sql":"SELECT * FROM staleStream","actions":[{"memory":{"topic":"%s"}}]}`, staleTopic2))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		r := TryAssert(20, ConstantInterval, func() bool {
			get, e := client.Get("connections/staleConn")
			s.Require().NoError(e)
			m, e := GetResponseResultMap(get)
			s.Require().NoError(e)
			return m["status"] == "connected"
		})
		s.Require().True(r)
	})

	s.Run("baseline: both rules receive data", func() {
		pub(0)
		s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(0)})
		s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(0)})
	})

	// The customer's scenario: restart rules that share the same connection/stream.
	// We try three restart patterns, each followed by a fresh data check, to surface
	// whichever variant triggers the stale-subscription race.

	s.Run("restart rule1 only, verify rule2 still receives", func() {
		resp, err := client.RestartRule("staleRule1")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(200 * time.Millisecond)
		drainChannel(subCh1)
		drainChannel(subCh2)

		pub(1)
		s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(1)})
		s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(1)})
	})

	s.Run("restart rule2 only, verify rule1 still receives", func() {
		resp, err := client.RestartRule("staleRule2")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(200 * time.Millisecond)
		drainChannel(subCh1)
		drainChannel(subCh2)

		pub(2)
		s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(2)})
		s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(2)})
	})

	s.Run("concurrent restart of both rules, verify both still receive", func() {
		errs := make(chan error, 2)
		for _, name := range []string{"staleRule1", "staleRule2"} {
			go func(name string) {
				resp, err := client.RestartRule(name)
				if err != nil {
					errs <- err
					return
				}
				body, _ := GetResponseText(resp)
				if resp.StatusCode != http.StatusOK {
					errs <- fmt.Errorf("restart %s: HTTP %d: %s", name, resp.StatusCode, body)
					return
				}
				errs <- nil
			}(name)
		}
		s.Require().NoError(<-errs)
		s.Require().NoError(<-errs)
		time.Sleep(200 * time.Millisecond)
		drainChannel(subCh1)
		drainChannel(subCh2)

		pub(3)
		s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(3)})
		s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(3)})
	})

	s.Run("rapid sequential restarts, verify both still receive", func() {
		for i := range 5 {
			resp, err := client.RestartRule("staleRule1")
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			resp, err = client.RestartRule("staleRule2")
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, resp.StatusCode)
			_ = i
		}
		time.Sleep(200 * time.Millisecond)
		drainChannel(subCh1)
		drainChannel(subCh2)

		pub(4)
		s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(4)})
		s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(4)})
	})

	// Targeted regression for the SubTopo eviction race:
	// Stop both rules (fully evicts the shared SubTopo from the pool).
	// Start only rule2, then rule1 slightly later, forcing rule1 to join
	// an already-running SubTopo that was recreated while its ref was absent.
	// Both rules must receive data — if the stale-ref bug is present rule1
	// will appear running but its Open call returns silently and no data flows.
	s.Run("full evict then staggered start, verify both receive", func() {
		// Stop both rules to evict the shared SubTopo completely.
		resp, err := client.Post("rules/staleRule1/stop", "")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		resp, err = client.Post("rules/staleRule2/stop", "")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(200 * time.Millisecond)

		// Start rule2 first so it creates and opens the shared SubTopo.
		resp, err = client.Post("rules/staleRule2/start", "")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(100 * time.Millisecond)

		// Start rule1 second — it must join the running SubTopo without its
		// Open call being silently dropped due to a CloseState race.
		resp, err = client.Post("rules/staleRule1/start", "")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(200 * time.Millisecond)
		drainChannel(subCh1)
		drainChannel(subCh2)

		pub(5)
		s.assertConnRecvMemTuple(subCh1, map[string]any{"v": float64(5)})
		s.assertConnRecvMemTuple(subCh2, map[string]any{"v": float64(5)})
	})

	s.Run("clean", func() {
		for _, rule := range []string{"staleRule1", "staleRule2"} {
			res, e := client.Delete("rules/" + rule)
			s.NoError(e)
			s.Equal(http.StatusOK, res.StatusCode)
		}
		res, e := client.Delete("streams/staleStream")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
		r := TryAssert(10, ConstantInterval, func() bool {
			res, e := client.Delete("connections/staleConn")
			s.NoError(e)
			return res.StatusCode == http.StatusOK
		})
		s.Require().True(r)
	})
}

// drainChannel discards all messages currently queued in ch without blocking.
func drainChannel(ch chan any) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (s *ConnectionTestSuite) assertConnRecvMemTuple(subCh chan any, expect map[string]any) {
	select {
	case d := <-subCh:
		mt, ok := d.([]pubsub.MemTuple)
		s.Require().True(ok, "expected []pubsub.MemTuple but got %T", d)
		s.Require().Len(mt, 1)
		s.Require().Equal(expect, mt[0].ToMap())
	case <-time.After(5 * time.Second):
		s.Fail("timeout waiting for memory tuple", "expected %v", expect)
	}
}
