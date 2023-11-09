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
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
)

func init() {
	testx.InitEnv("trial")
	factory.InitClientsFactory()
}

// Run two test rules in parallel. Rerun one of the rules
func TestTrialRule(t *testing.T) {
	// Test 1 wrong rule
	mockDef1 := `{"id":"rule1","sql":"select * from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1},{"name":"demo","value":2}],"interval":1,"loop":false}},"sinkProps":{"sendSingle":true}}`
	err := TrialManager.CreateRule(mockDef1)
	assert.Error(t, err)
	assert.Equal(t, "fail to run rule rule1: fail to get stream demo, please check if stream is created", err.Error())
	p := processor.NewStreamProcessor()
	_, err = p.ExecStmt("CREATE STREAM demo () WITH (DATASOURCE=\"demo\", TYPE=\"simulator\", FORMAT=\"json\", KEY=\"ts\")")
	assert.NoError(t, err)
	defer p.ExecStmt("DROP STREAM demo")
	var wg sync.WaitGroup

	// Test 2 valid rule with mock
	err = TrialManager.CreateRule(mockDef1)
	assert.NoError(t, err)
	// Read from ws
	u := url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/rule1"}
	c1, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	defer c1.Close()
	wg.Add(1)
	go func() {
		_ = c1.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, data, err := c1.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, `{"name":"demo","value":1}`, string(data))
		_, data, err = c1.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, `{"name":"demo","value":2}`, string(data))
		wg.Done()
	}()

	// Test 3 Runtime error rule
	mockDefErr := `{"id":"ruleErr","sql":"select name + value from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1},{"name":"demo","value":2}],"interval":1,"loop":true}},"sinkProps":{"sendSingle":true}}`
	err = TrialManager.CreateRule(mockDefErr)
	assert.NoError(t, err)
	// Read from ws
	u = url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/ruleErr"}
	c2, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	defer c2.Close()
	wg.Add(1)
	go func() {
		_ = c2.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, data, err := c2.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"error\":\"run Select error: expr: binaryExpr:{ demo.name + demo.value } meet error, err:invalid operation string(demo) + float64(1)\"}", string(data))
		wg.Done()
	}()

	// Test 4 Rule without mock
	noMockDef := `{"id":"rule2","sql":"select * from demo","sinkProps":{"sendSingle":true}}`
	err = TrialManager.CreateRule(noMockDef)
	assert.NoError(t, err)
	// Read from ws
	u = url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/rule2"}
	c3, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	defer c3.Close()
	wg.Add(1)
	go func() {
		_ = c3.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, data, err := c3.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"humidity\":50,\"temperature\":22.5}", string(data))
		_, data, err = c3.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"humidity\":50,\"temperature\":22.5}", string(data))
		wg.Done()
	}()

	err = TrialManager.StartRule("rule1")
	assert.NoError(t, err)
	err = TrialManager.StartRule("ruleErr")
	assert.NoError(t, err)
	err = TrialManager.StartRule("rule2")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(TrialManager.runs))
	wg.Wait()
	TrialManager.StopRule("ruleErr")
	TrialManager.StopRule("rule1")
	TrialManager.StopRule("rule2")
	assert.Equal(t, 0, len(TrialManager.runs))
}
