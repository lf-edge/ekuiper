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
			return metrics["source_tttStream_0_connection_status"] == -1.0
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
			return metrics["source_tttStream_0_connection_status"] == 1.0
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
			return metrics["source_tttStream_0_connection_status"] == 1.0
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
			return metrics["source_tttStream_0_connection_status"] == 0.0
		})
		s.Require().True(r)
		// Assert rule1 metrics
		r = TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleTTT2")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["source_tttStream_0_connection_status"] == 0.0
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
			return metrics["source_tttStream_0_connection_status"] == 1.0
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
			return metrics["source_tttStream_0_connection_status"] == 1.0
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
		{
			name: "sql",
			props: map[string]any{
				"url": "mysql://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
			},
			timeout: true,
			err:     "{\"error\":1000,\"message\":\"dial tcp 127.0.0.1:3306: connectex: No connection could be made because the target machine actively refused it.\"}\n",
		},
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
		{
			name: "sql",
			props: map[string]any{
				"url": "mysql://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
			},
			timeout: true,
			err:     "{\"error\":1000,\"message\":\"dial tcp 127.0.0.1:3306: connectex: No connection could be made because the target machine actively refused it.\"}\n",
		},
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
		{
			name: "sql",
			props: map[string]any{
				"url": "mysql://root:Q1w2e3r4t%25@test.com/test?parseTime=true",
			},
			timeout: true,
			// err: "{\"error\":1000,\"message\":\"dial tcp 127.0.0.1:3306: connectex: No connection could be made because the target machine actively refused it.\"}\n",
		},
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
