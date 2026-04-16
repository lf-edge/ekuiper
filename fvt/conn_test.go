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

func (s *ConnectionTestSuite) TestSharedConnectionPeerRuleStopImpactRepro() {
	const brokerAddr = ":5883"
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	connID := "connSharedStop" + suffix
	confKey := "sharedStopConf" + suffix
	streamName := "sharedStopStream" + suffix
	ruleActive := "ruleSharedStopActive" + suffix
	rulePeer := "ruleSharedStopPeer" + suffix
	sourceTpc := "fvt/shared/stop/" + suffix
	memActive := "fvt/shared/stop/active/" + suffix
	memPeer := "fvt/shared/stop/peer/" + suffix

	activeSub := pubsub.CreateSub(memActive, nil, memActive, 1024)
	defer pubsub.CloseSourceConsumerChannel(memActive, memActive)
	peerSub := pubsub.CreateSub(memPeer, nil, memPeer, 1024)
	defer pubsub.CloseSourceConsumerChannel(memPeer, memPeer)

	server, tcp := s.startInlineBroker("sharedStopBroker", brokerAddr)
	defer func() {
		err := server.Close()
		s.Require().NoError(err)
		tcp.Close(nil)
	}()

	s.createSharedConnectionArtifacts(connID, confKey, streamName, tcp.Address(), sourceTpc)
	defer s.cleanupSharedConnectionArtifacts(ruleActive, rulePeer, streamName, confKey, connID)

	s.createMemoryRule(ruleActive, streamName, memActive)
	s.createMemoryRule(rulePeer, streamName, memPeer)

	s.requireSharedSourceReady(server, sourceTpc, peerSub, activeSub, 1)

	resp, err := client.StopRule(rulePeer)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	err = server.Publish(sourceTpc, []byte(`{"seq":2}`), false, 0)
	s.Require().NoError(err)
	activeGot := s.waitForMemoryTuple(activeSub, 2, 3*time.Second)
	s.False(activeGot, "expected reproduction: after stopping peer rule, active rule should stop receiving on shared mqtt connection")
}

func (s *ConnectionTestSuite) TestSharedConnectionPeerRuleRestartImpactRepro() {
	const brokerAddr = ":5884"
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	connID := "connSharedRestart" + suffix
	confKey := "sharedRestartConf" + suffix
	streamName := "sharedRestartStream" + suffix
	ruleActive := "ruleSharedRestartActive" + suffix
	rulePeer := "ruleSharedRestartPeer" + suffix
	sourceTpc := "fvt/shared/restart/" + suffix
	memActive := "fvt/shared/restart/active/" + suffix
	memPeer := "fvt/shared/restart/peer/" + suffix

	activeSub := pubsub.CreateSub(memActive, nil, memActive, 1024)
	defer pubsub.CloseSourceConsumerChannel(memActive, memActive)
	peerSub := pubsub.CreateSub(memPeer, nil, memPeer, 1024)
	defer pubsub.CloseSourceConsumerChannel(memPeer, memPeer)

	server, tcp := s.startInlineBroker("sharedRestartBroker", brokerAddr)
	defer func() {
		err := server.Close()
		s.Require().NoError(err)
		tcp.Close(nil)
	}()

	s.createSharedConnectionArtifacts(connID, confKey, streamName, tcp.Address(), sourceTpc)
	defer s.cleanupSharedConnectionArtifacts(ruleActive, rulePeer, streamName, confKey, connID)

	s.createMemoryRule(ruleActive, streamName, memActive)
	s.createMemoryRule(rulePeer, streamName, memPeer)

	s.requireSharedSourceReady(server, sourceTpc, peerSub, activeSub, 1)

	resp, err := client.RestartRule(rulePeer)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	err = server.Publish(sourceTpc, []byte(`{"seq":2}`), false, 0)
	s.Require().NoError(err)
	peerGot := s.waitForMemoryTuple(peerSub, 2, 3*time.Second)
	activeGot := s.waitForMemoryTuple(activeSub, 2, 3*time.Second)
	s.False(peerGot && activeGot, "expected reproduction: after restarting peer rule, at least one rule should stop receiving on shared mqtt connection")
	s.T().Logf("restart reproduction result: peerGot=%v, activeGot=%v", peerGot, activeGot)
}

func (s *ConnectionTestSuite) startInlineBroker(id, addr string) (*mqtt.Server, *listeners.TCP) {
	server := mqtt.New(&mqtt.Options{InlineClient: true})
	_ = server.AddHook(new(auth.AllowHook), nil)
	tcp := listeners.NewTCP(listeners.Config{ID: id, Address: addr})
	err := server.AddListener(tcp)
	s.Require().NoError(err)
	go func() {
		err = server.Serve()
		fmt.Println(err)
	}()
	return server, tcp
}

func (s *ConnectionTestSuite) createSharedConnectionArtifacts(connID, confKey, streamName, brokerURL, sourceTopic string) {
	connStr := fmt.Sprintf(`{
		"id": %q,
		"typ":"mqtt",
		"props": {
			"server": %q,
			"protocolVersion": "3.1.1"
		}
	}`, connID, brokerURL)
	resp, err := client.Post("connections", connStr)
	s.Require().NoError(err)
	body, readErr := GetResponseText(resp)
	s.Require().NoError(readErr)
	s.Require().Equalf(http.StatusCreated, resp.StatusCode, "create connection failed: %s", body)

	conf := map[string]any{
		"connectionSelector": connID,
		"qos":                0,
	}
	resp, err = client.CreateConf("sources/mqtt/confKeys/"+confKey, conf)
	s.Require().NoError(err)
	body, readErr = GetResponseText(resp)
	s.Require().NoError(readErr)
	s.Require().Equalf(http.StatusOK, resp.StatusCode, "create source conf failed: %s", body)

	streamSQL := fmt.Sprintf(`{"sql": "create stream %s () WITH (TYPE=\"mqtt\", DATASOURCE=\"%s\", FORMAT=\"json\", CONF_KEY=\"%s\", SHARED=\"true\")"}`, streamName, sourceTopic, confKey)
	resp, err = client.CreateStream(streamSQL)
	s.Require().NoError(err)
	body, readErr = GetResponseText(resp)
	s.Require().NoError(readErr)
	s.Require().Equalf(http.StatusCreated, resp.StatusCode, "create stream failed: %s", body)
}

func (s *ConnectionTestSuite) createMemoryRule(ruleName, streamName, memoryTopic string) {
	ruleSQL := fmt.Sprintf(`{
	  "id": %q,
	  "sql": "SELECT * FROM %s",
	  "actions": [
		{
		  "memory": {
			"topic": %q
		  }
		}
	  ]
	}`, ruleName, streamName, memoryTopic)
	resp, err := client.CreateRule(ruleSQL)
	s.Require().NoError(err)
	body, readErr := GetResponseText(resp)
	s.Require().NoError(readErr)
	s.Require().Equalf(http.StatusCreated, resp.StatusCode, "create rule failed: %s", body)
}

func (s *ConnectionTestSuite) cleanupSharedConnectionArtifacts(ruleActive, rulePeer, streamName, confKey, connID string) {
	res, err := client.Delete("rules/" + rulePeer)
	s.NoError(err)
	if err == nil {
		s.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound)
	}

	res, err = client.Delete("rules/" + ruleActive)
	s.NoError(err)
	if err == nil {
		s.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound)
	}

	res, err = client.Delete("streams/" + streamName)
	s.NoError(err)
	if err == nil {
		s.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound)
	}

	res, err = client.Delete("metadata/sources/mqtt/confKeys/" + confKey)
	s.NoError(err)
	if err == nil {
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound {
			body, _ := GetResponseText(res)
			s.T().Logf("cleanup confKey %s returned %d: %s", confKey, res.StatusCode, body)
		}
	}

	_ = TryAssert(10, ConstantInterval, func() bool {
		res, err = client.Delete("connections/" + connID)
		s.NoError(err)
		if err != nil {
			return false
		}
		return res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound
	})
}

func (s *ConnectionTestSuite) requireMemoryTuple(ch chan any, seq int, timeout time.Duration) {
	s.Require().True(s.waitForMemoryTuple(ch, seq, timeout), "did not receive expected memory tuple")
}

func (s *ConnectionTestSuite) requireSharedSourceReady(server *mqtt.Server, sourceTopic string, peerSub, activeSub chan any, seq int) {
	deadline := time.Now().Add(5 * time.Second)
	peerReady := false
	activeReady := false
	payload := []byte(fmt.Sprintf(`{"seq":%d}`, seq))
	time.Sleep(300 * time.Millisecond)
	for time.Now().Before(deadline) && !(peerReady && activeReady) {
		err := server.Publish(sourceTopic, payload, false, 0)
		s.Require().NoError(err)
		if !peerReady {
			peerReady = s.waitForMemoryTuple(peerSub, seq, 300*time.Millisecond)
		}
		if !activeReady {
			activeReady = s.waitForMemoryTuple(activeSub, seq, 300*time.Millisecond)
		}
	}
	s.True(peerReady, "peer rule did not receive baseline tuple")
	s.True(activeReady, "active rule did not receive baseline tuple")
}

func (s *ConnectionTestSuite) waitForMemoryTuple(ch chan any, seq int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-ch:
			mt, ok := msg.([]pubsub.MemTuple)
			if !ok || len(mt) != 1 {
				continue
			}
			m := mt[0].ToMap()
			if v, ok := m["seq"]; ok {
				switch vt := v.(type) {
				case float64:
					if int(vt) == seq {
						return true
					}
				case int:
					if vt == seq {
						return true
					}
				}
			}
		case <-deadline:
			return false
		}
	}
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
