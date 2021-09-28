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
	"github.com/lf-edge/ekuiper/pkg/api"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	"sync"
	"time"
)

// Options Initialized in config
var Options = map[string]interface{}{
	mangos.OptionSendDeadline: 1000,
}

type Closable interface {
	Close() error
}

type ControlChannel interface {
	Handshake() error
	SendCmd(arg []byte) error
	Closable
}

// NanomsgReqChannel shared by symbols
type NanomsgReqChannel struct {
	sync.Mutex
	sock mangos.Socket
}

func (r *NanomsgReqChannel) Close() error {
	return r.sock.Close()
}

func (r *NanomsgReqChannel) SendCmd(arg []byte) error {
	r.Lock()
	defer r.Unlock()
	if err := r.sock.Send(arg); err != nil {
		return fmt.Errorf("can't send message on control rep socket: %s", err.Error())
	}
	if msg, err := r.sock.Recv(); err != nil {
		return fmt.Errorf("can't receive: %s", err.Error())
	} else {
		if string(msg) != "ok" {
			return fmt.Errorf("receive error: %s", string(msg))
		}
	}
	return nil
}

// Handshake should only be called once
func (r *NanomsgReqChannel) Handshake() error {
	_, err := r.sock.Recv()
	return err
}

type DataInChannel interface {
	Recv() ([]byte, error)
	Closable
}

type DataOutChannel interface {
	Send([]byte) error
	Closable
}

type DataReqChannel interface {
	Handshake() error
	Req([]byte) ([]byte, error)
	Closable
}

type NanomsgReqRepChannel struct {
	sync.Mutex
	sock mangos.Socket
}

func (r *NanomsgReqRepChannel) Close() error {
	return r.sock.Close()
}

func (r *NanomsgReqRepChannel) Req(arg []byte) ([]byte, error) {
	r.Lock()
	defer r.Unlock()
	if err := r.sock.Send(arg); err != nil {
		return nil, fmt.Errorf("can't send message on function rep socket: %s", err.Error())
	}
	return r.sock.Recv()
}

// Handshake should only be called once
func (r *NanomsgReqRepChannel) Handshake() error {
	_, err := r.sock.Recv()
	return err
}

func CreateSourceChannel(ctx api.StreamContext) (DataInChannel, error) {
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

func CreateFunctionChannel(symbolName string) (DataReqChannel, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = rep.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new rep socket: %s", err)
	}
	setSockOptions(sock)
	sock.SetOption(mangos.OptionRecvDeadline, 1000)
	url := fmt.Sprintf("ipc:///tmp/func_%s.ipc", symbolName)
	if err = listenWithRetry(sock, url); err != nil {
		return nil, fmt.Errorf("can't listen on rep socket for %s: %s", url, err.Error())
	}
	return &NanomsgReqRepChannel{sock: sock}, nil
}

func CreateSinkChannel(ctx api.StreamContext) (DataOutChannel, error) {
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

func CreateControlChannel(pluginName string) (ControlChannel, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = rep.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new rep socket: %s", err)
	}
	setSockOptions(sock)
	sock.SetOption(mangos.OptionRecvDeadline, 100)
	url := fmt.Sprintf("ipc:///tmp/plugin_%s.ipc", pluginName)
	if err = listenWithRetry(sock, url); err != nil {
		return nil, fmt.Errorf("can't listen on rep socket: %s", err.Error())
	}
	return &NanomsgReqChannel{sock: sock}, nil
}

func setSockOptions(sock mangos.Socket) {
	for k, v := range Options {
		sock.SetOption(k, v)
	}
}

func listenWithRetry(sock mangos.Socket, url string) error {
	var (
		retryCount    = 300
		retryInterval = 100
	)
	for {
		err := sock.Listen(url)
		if err == nil {
			conf.Log.Infof("start to listen at %s after %d tries", url, 301-retryCount)
			return err
		}
		retryCount--
		if retryCount < 0 {
			return err
		}
		time.Sleep(time.Duration(retryInterval) * time.Millisecond)
	}
}
