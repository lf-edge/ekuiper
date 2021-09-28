// Copyright 2021 EMQ Technologies Co., Ltd.
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

package runtime

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/req"
	"testing"
)

// Plugin manager involves process, only covered in the integration test

// TestPluginInstance test the encode/decode of command
func TestPluginInstance(t *testing.T) {
	pluginName := "test"
	ch, err := CreateControlChannel(pluginName)
	if err != nil {
		t.Errorf("create channel error %v", err)
		return
	}
	client, err := createMockClient(pluginName)
	if err != nil {
		t.Errorf("normal process: create client error %v", err)
		return
	}
	err = client.Send([]byte("handshake"))
	if err != nil {
		t.Errorf("can't send handshake: %s", err.Error())
		return
	}
	err = ch.Handshake()
	if err != nil {
		t.Errorf("can't ack handshake: %s", err.Error())
		return
	}
	ins := &PluginIns{
		name:     "test",
		process:  nil,
		ctrlChan: ch,
	}
	var tests = []struct {
		c  *Control
		sj string
		ej string
	}{
		{
			c: &Control{
				SymbolName: "symbol1",
				Meta: &Meta{
					RuleId:     "rule1",
					OpId:       "op1",
					InstanceId: 0,
				},
				PluginType: "sources",
				DataSource: "topic",
				Config:     map[string]interface{}{"abc": 1},
			},
			sj: "{\"cmd\":\"start\",\"arg\":\"{\\\"symbolName\\\":\\\"symbol1\\\",\\\"meta\\\":{\\\"ruleId\\\":\\\"rule1\\\",\\\"opId\\\":\\\"op1\\\",\\\"instanceId\\\":0},\\\"pluginType\\\":\\\"sources\\\",\\\"dataSource\\\":\\\"topic\\\",\\\"config\\\":{\\\"abc\\\":1}}\"}",
			ej: "{\"cmd\":\"stop\",\"arg\":\"{\\\"symbolName\\\":\\\"symbol1\\\",\\\"meta\\\":{\\\"ruleId\\\":\\\"rule1\\\",\\\"opId\\\":\\\"op1\\\",\\\"instanceId\\\":0},\\\"pluginType\\\":\\\"sources\\\",\\\"dataSource\\\":\\\"topic\\\",\\\"config\\\":{\\\"abc\\\":1}}\"}",
		}, {
			c: &Control{
				SymbolName: "symbol2",
				Meta: &Meta{
					RuleId:     "rule1",
					OpId:       "op2",
					InstanceId: 0,
				},
				PluginType: "functions",
			},
			sj: "{\"cmd\":\"start\",\"arg\":\"{\\\"symbolName\\\":\\\"symbol2\\\",\\\"meta\\\":{\\\"ruleId\\\":\\\"rule1\\\",\\\"opId\\\":\\\"op2\\\",\\\"instanceId\\\":0},\\\"pluginType\\\":\\\"functions\\\"}\"}",
			ej: "{\"cmd\":\"stop\",\"arg\":\"{\\\"symbolName\\\":\\\"symbol2\\\",\\\"meta\\\":{\\\"ruleId\\\":\\\"rule1\\\",\\\"opId\\\":\\\"op2\\\",\\\"instanceId\\\":0},\\\"pluginType\\\":\\\"functions\\\"}\"}",
		}, {
			c: &Control{
				SymbolName: "symbol3",
				Meta: &Meta{
					RuleId:     "rule1",
					OpId:       "op3",
					InstanceId: 0,
				},
				PluginType: "sinks",
				Config:     map[string]interface{}{"def": map[string]interface{}{"ci": "aaa"}},
			},
			sj: "{\"cmd\":\"start\",\"arg\":\"{\\\"symbolName\\\":\\\"symbol3\\\",\\\"meta\\\":{\\\"ruleId\\\":\\\"rule1\\\",\\\"opId\\\":\\\"op3\\\",\\\"instanceId\\\":0},\\\"pluginType\\\":\\\"sinks\\\",\\\"config\\\":{\\\"def\\\":{\\\"ci\\\":\\\"aaa\\\"}}}\"}",
			ej: "{\"cmd\":\"stop\",\"arg\":\"{\\\"symbolName\\\":\\\"symbol3\\\",\\\"meta\\\":{\\\"ruleId\\\":\\\"rule1\\\",\\\"opId\\\":\\\"op3\\\",\\\"instanceId\\\":0},\\\"pluginType\\\":\\\"sinks\\\",\\\"config\\\":{\\\"def\\\":{\\\"ci\\\":\\\"aaa\\\"}}}\"}",
		},
	}
	ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log)
	sctx := ctx.WithMeta("rule1", "op1", &state.MemoryStore{}).WithInstance(1)
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	go func() {
		err := ins.StartSymbol(sctx, tests[0].c)
		if err != nil {
			t.Errorf("start command err %v", err)
			return
		}
		for _, tt := range tests {
			err := ins.StartSymbol(sctx, tt.c)
			if err != nil {
				t.Errorf("start command err %v", err)
				return
			}
			err = ins.StopSymbol(sctx, tt.c)
			if err != nil {
				t.Errorf("stop command err %v", err)
				return
			}
		}
	}()
	// start symbol1 to avoild instance clean
	msg, err := client.Recv()
	if err != nil {
		t.Errorf("receive start command err %v", err)
	}
	client.Send(okMsg)
	sj := string(msg)
	if sj != tests[0].sj {
		t.Errorf("start command mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", tests[0].sj, sj)
	}
	for _, tt := range tests {
		msg, err := client.Recv()
		if err != nil {
			t.Errorf("receive start command err %v", err)
			break
		}
		client.Send(okMsg)
		sj := string(msg)
		if sj != tt.sj {
			t.Errorf("start command mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", tt.sj, sj)
		}
		msg, err = client.Recv()
		if err != nil {
			t.Errorf("receive stop command err %v", err)
			break
		}
		client.Send(okMsg)
		ej := string(msg)
		if ej != tt.ej {
			t.Errorf("end command mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", tt.ej, ej)
		}
	}
	err = client.Close()
	if err != nil {
		t.Errorf("close client error %v", err)
	}
	err = ins.ctrlChan.Close()
	if err != nil {
		t.Errorf("close ins error %v", err)
	}
}

func createMockClient(pluginName string) (mangos.Socket, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = req.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new req socket: %s", err)
	}
	setSockOptions(sock)
	url := fmt.Sprintf("ipc:///tmp/plugin_%s.ipc", pluginName)
	if err = sock.Dial(url); err != nil {
		return nil, fmt.Errorf("can't dial on req socket: %s", err.Error())
	}
	return sock, nil
}
