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
	"github.com/stretchr/testify/require"

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
	p := processor.NewStreamProcessor()
	p.ExecStmt("DROP STREAM demo")
	// Test 1 wrong rule
	mockDef1 := `{"id":"rule1","sql":"select * from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1},{"name":"demo","value":2}],"interval":1,"loop":false}},"sinkProps":{"sendSingle":true}}`
	_, err := TrialManager.CreateRule(mockDef1)
	assert.Error(t, err)
	assert.Equal(t, "fail to run rule rule1: fail to get stream demo, please check if stream is created", err.Error())

	_, err = p.ExecStmt("CREATE STREAM demo () WITH (DATASOURCE=\"demo\", TYPE=\"simulator\", FORMAT=\"json\", KEY=\"ts\")")
	assert.NoError(t, err)
	defer p.ExecStmt("DROP STREAM demo")

	wrongTpDef := `{"id":"rule5","sql":"select * from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1},{"name":"demo","value":2}],"interval":1,"loop":false}},"sinkProps":{"sendSingle":true,"dataTemplate":"a{{.{{}name}}c"}}`
	_, err = TrialManager.CreateRule(wrongTpDef)
	assert.Error(t, err)
	assert.Equal(t, "fail to run rule rule5: property dataTemplate a{{.{{}name}}c is invalid: template: sink:1: bad character U+007B '{'", err.Error())

	var wg sync.WaitGroup

	// Test 2 valid rule with mock
	id, err := TrialManager.CreateRule(mockDef1)
	assert.NoError(t, err)
	assert.Equal(t, "rule1", id)
	// wait server ready
	time.Sleep(10 * time.Millisecond)
	// Read from ws
	u := url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/rule1"}
	c1, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	err = TrialManager.StartRule(id)
	require.NoError(t, err)

	defer c1.Close()
	wg.Add(1)
	go func() {
		c1.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := c1.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, `{"name":"demo","value":1}`, string(data))
		_, data, err = c1.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, `{"name":"demo","value":2}`, string(data))
		wg.Done()
	}()

	// Test 3 Runtime error rule
	mockDefErr := `{"id":"ruleErr","sql":"select name + value from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1}],"interval":1,"loop":true}},"sinkProps":{"sendSingle":true}}`
	id, err = TrialManager.CreateRule(mockDefErr)
	assert.NoError(t, err)
	assert.Equal(t, "ruleErr", id)
	// wait server ready
	time.Sleep(10 * time.Millisecond)
	// Read from ws
	u = url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/ruleErr"}
	c2, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	defer c2.Close()
	err = TrialManager.StartRule(id)
	require.NoError(t, err)
	wg.Add(1)
	go func() {
		c2.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := c2.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"error\":\"run Select error: expr: binaryExpr:{ demo.name + demo.value } meet error, err:invalid operation string(demo) + float64(1)\"}", string(data))
		wg.Done()
	}()

	// Test 4 Rule without mock
	noMockDef := `{"id":"rule2","sql":"select * from demo","sinkProps":{"sendSingle":true}}`
	id, err = TrialManager.CreateRule(noMockDef)
	assert.Equal(t, "rule2", id)
	assert.NoError(t, err)
	// wait server ready
	time.Sleep(10 * time.Millisecond)
	// Read from ws
	u = url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/rule2"}
	c3, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	defer c3.Close()
	err = TrialManager.StartRule(id)
	require.NoError(t, err)
	wg.Add(1)
	go func() {
		c3.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := c3.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"humidity\":50,\"temperature\":22.5}", string(data))
		_, data, err = c3.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"humidity\":50,\"temperature\":22.5}", string(data))
		wg.Done()
	}()

	// Test 5 wrong template
	wrongTpDef = `{"id":"rule5","sql":"select * from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1},{"name":"demo","value":2}],"interval":1,"loop":false}},"sinkProps":{"sendSingle":true,"dataTemplate":"abc","dataField":"a"}}`
	id, err = TrialManager.CreateRule(wrongTpDef)
	assert.NoError(t, err)
	assert.Equal(t, "rule5", id)
	// wait server ready
	time.Sleep(10 * time.Millisecond)
	// Read from ws
	u = url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/rule5"}
	c4, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	err = TrialManager.StartRule(id)
	require.NoError(t, err)

	defer c4.Close()
	wg.Add(1)
	go func() {
		c4.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := c4.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, "{\"error\":\"fail to TransItem data map[name:demo value:1] for error fail to decode data abc for error invalid character 'a' looking for beginning of value\"}", string(data))
		wg.Done()
	}()

	assert.Equal(t, 4, len(TrialManager.runs))
	wg.Wait()
	TrialManager.StopRule("ruleErr")
	TrialManager.StopRule("rule1")
	TrialManager.StopRule("rule2")
	TrialManager.StopRule("rule5")
	assert.Equal(t, 0, len(TrialManager.runs))
}
