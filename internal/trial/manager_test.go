// Copyright 2023 EMQ Technologies Co., Ltd.
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

package trial

import (
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// Run two test rules in parallel. Rerun one of the rules
func TestTrialRule(t *testing.T) {
	ip := "127.0.0.1"
	port := 10087
	httpserver.InitGlobalServerManager(ip, port, nil)
	connection.InitConnectionManager4Test()
	conf.IsTesting = true
	conf.InitConf()
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	p := processor.NewStreamProcessor()
	p.ExecStmt("DROP STREAM demo")
	// Test 1 wrong rule
	mockDef1 := `{"id":"rule1","sql":"select * from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1}],"interval":100,"loop":true}},"sinkProps":{"sendSingle":true}}`
	_, err = TrialManager.CreateRule(mockDef1)
	require.Error(t, err)
	require.Equal(t, "fail to run rule rule1: fail to get stream demo, please check if stream is created", err.Error())

	_, err = p.ExecStmt("CREATE STREAM demo () WITH (DATASOURCE=\"demo\", TYPE=\"simulator\", FORMAT=\"json\", KEY=\"ts\")")
	require.NoError(t, err)
	defer p.ExecStmt("DROP STREAM demo")

	// Test 2 valid rule with mock
	testValidTrial(t, mockDef1)

	// Test 3 Runtime error rule
	testRuntimeErrorTrial(t)

	// Test 4 Rule without mock
	testRealSourceTrial(t)
	require.Equal(t, 0, len(TrialManager.runs))
}

func testValidTrial(t *testing.T, mockDef1 string) {
	// Test 2 valid rule with mock
	id, err := TrialManager.CreateRule(mockDef1)
	require.NoError(t, err)
	require.Equal(t, "rule1", id)
	// Read from ws
	u := url.URL{Scheme: "ws", Host: "localhost:10087", Path: "/test/rule1"}
	c1, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	recvCh := make(chan []byte, 10)
	closeCh := make(chan struct{}, 10)
	go func() {
		_, data, err := c1.ReadMessage()
		require.NoError(t, err)
		recvCh <- data
	}()
	go func() {
		for {
			select {
			case <-closeCh:
				return
			default:
				timex.Add(100 * time.Millisecond)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, TrialManager.StartRule("rule1"))
	require.Equal(t, []byte(`{"name":"demo","value":1}`), <-recvCh)
	c1.Close()
	TrialManager.StopRule("rule1")
	closeCh <- struct{}{}
}

func testRuntimeErrorTrial(t *testing.T) {
	// Test 3 Runtime error rule
	mockDefErr := `{"id":"ruleErr","sql":"select name + value from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1}],"interval":100,"loop":true}},"sinkProps":{"sendSingle":true}}`
	id, err := TrialManager.CreateRule(mockDefErr)
	require.NoError(t, err)
	require.Equal(t, "ruleErr", id)
	// Read from ws
	u := url.URL{Scheme: "ws", Host: "localhost:10087", Path: "/test/ruleErr"}
	c2, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	recvCh := make(chan []byte, 10)
	closeCh := make(chan struct{}, 10)
	go func() {
		_, data, err := c2.ReadMessage()
		require.NoError(t, err)
		recvCh <- data
	}()
	go func() {
		for {
			select {
			case <-closeCh:
				return
			default:
				timex.Add(100 * time.Millisecond)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, TrialManager.StartRule(id))
	require.Equal(t, `run Select error: expr: binaryExpr:{ demo.name + demo.value } meet error, err:invalid operation string(demo) + float64(1)`, string(<-recvCh))
	TrialManager.StopRule(id)
	closeCh <- struct{}{}
	c2.Close()
}

func testRealSourceTrial(t *testing.T) {
	noMockDef := `{"id":"rule2","sql":"select * from demo","sinkProps":{"sendSingle":true}}`
	id, err := TrialManager.CreateRule(noMockDef)
	assert.Equal(t, "rule2", id)
	assert.NoError(t, err)

	u := url.URL{Scheme: "ws", Host: "localhost:10087", Path: "/test/rule2"}
	c3, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	recvCh := make(chan []byte, 10)
	closeCh := make(chan struct{}, 10)
	go func() {
		_, data, err := c3.ReadMessage()
		require.NoError(t, err)
		recvCh <- data
	}()
	go func() {
		for {
			select {
			case <-closeCh:
				return
			default:
				timex.Add(time.Second)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, TrialManager.StartRule(id))
	require.Equal(t, "{\"humidity\":50,\"temperature\":22.5}", string(<-recvCh))
	TrialManager.StopRule(id)
	closeCh <- struct{}{}
	c3.Close()
}
