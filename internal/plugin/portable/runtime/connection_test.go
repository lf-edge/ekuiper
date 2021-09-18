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
	"github.com/lf-edge/ekuiper/pkg/api"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/req"
	"reflect"
	"testing"
)

var okMsg = []byte("ok")

func TestControlCh(t *testing.T) {
	pluginName := "test"
	// 1. normal process
	ch, err := CreateControlChannel(pluginName)
	if err != nil {
		t.Errorf("normal process: create channel error %v", err)
		return
	}
	client, err := createMockControlChannel(pluginName)
	if err != nil {
		t.Errorf("normal process: create client error %v", err)
		return
	}
	clientStopped := false
	go func() {
		err := client.Run()
		if err != nil && !clientStopped {
			t.Errorf("normal process: client error %v", err)
		}
		fmt.Printf("exiting normal client\n")
	}()

	err = ch.Handshake()
	if err != nil {
		t.Errorf("normal process: handshake error %v", err)
	}
	sendCount := 0
	for {
		sendCount++
		err = ch.SendCmd(okMsg)
		if err != nil {
			t.Errorf("normal process: %d sendCmd error %v", sendCount, err)
		}
		if sendCount >= 3 {
			break
		}
	}
	err = ch.Close()
	if err != nil {
		t.Errorf("normal process: close error %v", err)
	}
	// 2. client not closed, channel is still occupied?
	ch, err = CreateControlChannel(pluginName)
	if err != nil {
		t.Errorf("2nd process: recreate channel error %v", err)
	}
	// 3. server not started
	err = ch.Close()
	if err != nil {
		t.Errorf("normal process: close error %v", err)
	}
	clientStopped = true
	err = client.Close()
	if err != nil {
		t.Errorf("3rd process: close client error %v", err)
	}
	_, err = createMockControlChannel(pluginName)
	if err == nil || err.Error() == "" {
		t.Errorf("3rd process: create client should have error but got %v", err)
		return
	}
	// 4. double control channel client
	ch, err = CreateControlChannel(pluginName)
	if err != nil {
		t.Errorf("4th process: create channel error %v", err)
	}
	client, err = createMockControlChannel(pluginName)
	if err != nil {
		t.Errorf("4th process: create client error %v", err)
	}
	clientStopped = false
	go func() {
		err := client.Run()
		if err != nil && !clientStopped {
			t.Errorf("4th process: client error %v", err)
		}
		fmt.Printf("exiting 4th process client\n")
	}()

	// 5. no handshake
	err = ch.SendCmd(okMsg)
	if err == nil || err.Error() != "can't send message on control rep socket: incorrect protocol state" {
		t.Errorf("5th process: send command should have error but got %v", err)
	}
	err = ch.Handshake()
	if err != nil {
		t.Errorf("5th process: handshake error %v", err)
	}
	err = ch.SendCmd(okMsg)
	if err != nil {
		t.Errorf("5th process: sendCmd error %v", err)
	}
	err = ch.Close()
	if err != nil {
		t.Errorf("5th process: close error %v", err)
	}
	clientStopped = true
	err = client.Close()
	if err != nil {
		t.Errorf("5th process: client close error %v", err)
	}
}

func TestDataIn(t *testing.T) {
	i := 0
	ctx := context.DefaultContext{}
	sctx := ctx.WithMeta("rule1", "op1", &state.MemoryStore{}).WithInstance(1)
	for i < 2 { // normal start and restart
		ch, err := CreateSourceChannel(sctx)
		if err != nil {
			t.Errorf("phase %d create channel error %v", i, err)
		}
		client, err := createMockSourceChannel(sctx)
		if err != nil {
			t.Errorf("phase %d create client error %v", i, err)
		}
		go func() {
			var c = 0
			for c < 3 {
				err := client.Send(okMsg)
				if err != nil {
					t.Errorf("phase %d client send error %v", i, err)
					return
				}
				conf.Log.Debugf("phase %d sent %d messages", i, c)
				c++
			}
		}()
		var c = 0
		for c < 3 {
			msg, err := ch.Recv()
			if err != nil {
				t.Errorf("phase %d receive error %v", i, err)
				return
			}
			if !reflect.DeepEqual(msg, okMsg) {
				t.Errorf("phase %d receive %s but expect %s", i, msg, okMsg)
			}
			c++
		}
		err = ch.Close()
		if err != nil {
			t.Errorf("phase %d close error %v", i, err)
		}
		client.Close()
		if err != nil {
			t.Errorf("phase %d close client error %v", i, err)
		}
		i++
	}
}

func TestDataOut(t *testing.T) {
	i := 0
	ctx := context.DefaultContext{}
	sctx := ctx.WithMeta("rule1", "op1", &state.MemoryStore{}).WithInstance(1)
	for i < 2 { // normal start and restart
		client, err := createMockSinkChannel(sctx)
		if err != nil {
			t.Errorf("phase %d create client error %v", i, err)
		}
		ch, err := CreateSinkChannel(sctx)
		if err != nil {
			t.Errorf("phase %d create channel error %v", i, err)
		}
		go func() {
			var c = 0
			for c < 3 {
				err := ch.Send(okMsg)
				if err != nil {
					t.Errorf("phase %d client send error %v", i, err)
					return
				}
				conf.Log.Debugf("phase %d sent %d messages", i, c)
				c++
			}
		}()
		var c = 0
		for c < 3 {
			msg, err := client.Recv()
			if err != nil {
				t.Errorf("phase %d receive error %v", i, err)
				return
			}
			if !reflect.DeepEqual(msg, okMsg) {
				t.Errorf("phase %d receive %s but expect %s", i, msg, okMsg)
			}
			c++
		}
		err = ch.Close()
		if err != nil {
			t.Errorf("phase %d close error %v", i, err)
		}
		client.Close()
		if err != nil {
			t.Errorf("phase %d close client error %v", i, err)
		}
		i++
	}
}

type mockControlClient struct {
	sock mangos.Socket
}

// Run until process end
func (r *mockControlClient) Run() error {
	err := r.sock.Send([]byte("handshake"))
	if err != nil {
		return fmt.Errorf("can't send handshake: %s", err.Error())
	}
	for {
		msg, err := r.sock.Recv()
		if err != nil {
			return fmt.Errorf("cannot receive on rep socket: %s", err.Error())
		}
		if !reflect.DeepEqual(msg, okMsg) {
			return fmt.Errorf("control client recieve %s but expect %s", string(msg), string(okMsg))
		}
		err = r.sock.Send(okMsg)
		if err != nil {
			return fmt.Errorf("can't send reply: %s", err.Error())
		}
	}
	return nil
}

func (r *mockControlClient) Close() error {
	return r.sock.Close()
}

func createMockControlChannel(pluginName string) (*mockControlClient, error) {
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
	return &mockControlClient{sock: sock}, nil
}

func createMockSourceChannel(ctx api.StreamContext) (mangos.Socket, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = push.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new push socket: %s", err)
	}
	setSockOptions(sock)
	url := fmt.Sprintf("ipc:///tmp/%s_%s_%d.ipc", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	if err = sock.Dial(url); err != nil {
		return nil, fmt.Errorf("can't dial on push socket: %s", err.Error())
	}
	return sock, nil
}

func createMockSinkChannel(ctx api.StreamContext) (mangos.Socket, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = pull.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new pull socket: %s", err)
	}
	setSockOptions(sock)
	url := fmt.Sprintf("ipc:///tmp/%s_%s_%d.ipc", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	if err = listenWithRetry(sock, url); err != nil {
		return nil, fmt.Errorf("can't listen on pull socket for %s: %s", url, err.Error())
	}
	return sock, nil
}
