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
	"fmt"
	"net/http"
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
