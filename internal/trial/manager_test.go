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
	p := processor.NewStreamProcessor()
	_, err := p.ExecStmt("CREATE STREAM demo () WITH (DATASOURCE=\"demo\", TYPE=\"mock\", FORMAT=\"json\", KEY=\"ts\")")
	assert.NoError(t, err)
	defer p.ExecStmt("DROP STREAM demo")
	mockDef1 := `{"id":"rule1","sql":"select * from demo","mockSource":{"demo":{"data":[{"name":"demo","value":1}],"interval":1,"loop":true}},"sinkProps":{"sendSingle":true}}`
	err = TrialManager.AddRule(mockDef1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(TrialManager.runs))
	// Read from ws
	u := url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/test/rule1"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(t, err)
	defer c.Close()
	_, data, err := c.ReadMessage()
	assert.NoError(t, err)
	assert.Equal(t, `{"name":"demo","value":1}`, string(data))
	_, data, err = c.ReadMessage()
	assert.NoError(t, err)
	assert.Equal(t, `{"name":"demo","value":1}`, string(data))

	TrialManager.StopRule("rule1")
	assert.Equal(t, 0, len(TrialManager.runs))
}
