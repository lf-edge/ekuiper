// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/req"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
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
	ins := NewPluginIns("test", ch, nil)
	tests := []struct {
		c  *Control
		sj string
		ej string
	}{
		{
			c: &Control{
				SymbolName: "symbol1",
				Meta: Meta{
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
				Meta: Meta{
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
				Meta: Meta{
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := ins.Status.GetRuleRefCount("rule1")
		require.Equal(t, 0, count)
		err := ins.StartSymbol(sctx, tests[0].c)
		if err != nil {
			t.Errorf("start command err %v", err)
			return
		}
		count = ins.Status.GetRuleRefCount("rule1")
		require.Equal(t, 1, count)
		for _, tt := range tests {
			pCnt := ins.Status.GetRuleRefCount("rule1")
			err := ins.StartSymbol(sctx, tt.c)
			if err != nil {
				t.Errorf("start command err %v", err)
				return
			}
			err = ins.StopSymbol(sctx, tt.c)
			if err != nil {
				fmt.Printf("stop command err %v\n", err)
				continue
			}
			require.Equal(t, pCnt, ins.Status.GetRuleRefCount("rule1"))
		}
	}()
	// start symbol1 to avoid instance clean
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
	wg.Wait()
}

func createMockClient(pluginName string) (mangos.Socket, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = req.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new req socket: %s", err)
	}
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionRetryTime: 0,
	})
	url := fmt.Sprintf("ipc:///tmp/plugin_%s.ipc", pluginName)
	if err = sock.Dial(url); err != nil {
		return nil, fmt.Errorf("can't dial on req socket: %s", err.Error())
	}
	return sock, nil
}

func TestPluginStatus(t *testing.T) {
	p := NewPluginIns("mock", nil, nil)
	require.Equal(t, PluginStatusInit, p.GetStatus().Status)
	p.Status.StartRunning()
	require.Equal(t, PluginStatusRunning, p.GetStatus().Status)
	p.Status.StatusErr(errors.New("mock"))
	require.Equal(t, PluginStatusErr, p.GetStatus().Status)
	p.Status.Stop()
	require.Equal(t, PluginStatusStop, p.GetStatus().Status)
}

func TestPluginStatusRef(t *testing.T) {
	p := NewPluginIns("mock", nil, nil)
	s, _ := state.CreateStore("rule1", def.AtMostOnce)
	ctx := context.Background().WithMeta("rule1", "2", s)
	p.addRef(ctx)
	require.Equal(t, map[string]int{"rule1": 1}, p.GetStatus().RefCount)
	p.addRef(ctx)
	require.Equal(t, map[string]int{"rule1": 2}, p.GetStatus().RefCount)
	p.deRef(ctx)
	require.Equal(t, map[string]int{"rule1": 1}, p.GetStatus().RefCount)
	p.deRef(ctx)
	require.Equal(t, map[string]int{}, p.GetStatus().RefCount)
}
