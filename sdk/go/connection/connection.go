// Copyright 2022 EMQ Technologies Co., Ltd.
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

package connection

import (
	"fmt"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/req"
	// introduce ipc
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/lf-edge/ekuiper/sdk/go/context"
)

// Options Initialized in plugin.go Start according to the config
var (
	dialOptions = map[string]interface{}{
		mangos.OptionDialAsynch:       false,
		mangos.OptionMaxReconnectTime: 5 * time.Second,
		mangos.OptionReconnectTime:    100 * time.Millisecond,
	}
)

type Closable interface {
	Close() error
}

type ReplyFunc func([]byte) []byte

type ControlChannel interface {
	// reply with string message
	Run(ReplyFunc) error
	Closable
}

type DataInChannel interface {
	Recv() ([]byte, error)
	Closable
}

type DataOutChannel interface {
	Send([]byte) error
	Closable
}

type DataInOutChannel interface {
	Run(ReplyFunc) error
	Closable
}

type NanomsgRepChannel struct {
	sock mangos.Socket
}

// Run until process end
func (r *NanomsgRepChannel) Run(f ReplyFunc) error {
	err := r.sock.Send([]byte("handshake"))
	if err != nil {
		return fmt.Errorf("can't send handshake: %s", err.Error())
	}
	for {
		msg, err := r.sock.Recv()
		if err != nil {
			return fmt.Errorf("cannot receive on rep socket: %s", err.Error())
		}
		reply := f(msg)
		err = r.sock.Send(reply)
		if err != nil {
			return fmt.Errorf("can't send reply: %s", err.Error())
		}
	}
}

func (r *NanomsgRepChannel) Close() error {
	return r.sock.Close()
}

func CreateControlChannel(pluginName string) (ControlChannel, error) {
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
	if err = sock.DialOptions(url, dialOptions); err != nil {
		return nil, fmt.Errorf("can't dial on req socket: %s", err.Error())
	}
	return &NanomsgRepChannel{sock: sock}, nil
}

func CreateSourceChannel(ctx api.StreamContext) (DataOutChannel, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = push.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new push socket: %s", err)
	}
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionSendDeadline: 1000 * time.Millisecond,
	})
	url := fmt.Sprintf("ipc:///tmp/%s_%s_%d.ipc", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	if err = sock.DialOptions(url, dialOptions); err != nil {
		return nil, fmt.Errorf("can't dial on push socket: %s", err.Error())
	}
	return sock, nil
}

func CreateFuncChannel(symbolName string) (DataInOutChannel, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = req.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new req socket: %s", err)
	}
	// The recv should not have timeout because it is event driven
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionSendDeadline: 1000 * time.Millisecond,
		mangos.OptionRetryTime:    0,
	})
	url := fmt.Sprintf("ipc:///tmp/func_%s.ipc", symbolName)
	if err = sock.DialOptions(url, dialOptions); err != nil {
		return nil, fmt.Errorf("can't dial on req socket: %s", err.Error())
	}
	return &NanomsgRepChannel{sock: sock}, nil
}

func CreateSinkChannel(ctx api.StreamContext) (DataInChannel, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = pull.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new pull socket: %s", err)
	}
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionRecvDeadline: 500 * time.Millisecond,
	})
	url := fmt.Sprintf("ipc:///tmp/%s_%s_%d.ipc", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	if err = listenWithRetry(sock, url); err != nil {
		return nil, fmt.Errorf("can't listen on pull socket for %s: %s", url, err.Error())
	}
	return sock, nil
}

func CreateSinkAckChannel(ctx api.StreamContext) (DataOutChannel, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = push.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new push socket: %s", err)
	}
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionSendDeadline: 1000 * time.Millisecond,
	})
	url := fmt.Sprintf("ipc:///tmp/%s_%s_%d_ack.ipc", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	if err = sock.DialOptions(url, dialOptions); err != nil {
		return nil, fmt.Errorf("can't dial on push socket: %s", err.Error())
	}
	return sock, nil
}

func setSockOptions(sock mangos.Socket, sockOptions map[string]interface{}) {
	for k, v := range sockOptions {
		err := sock.SetOption(k, v)
		if err != nil && err != mangos.ErrBadOption {
			context.Log.Errorf("can't set socket option %s: %s", k, err.Error())
		}
	}
}

func listenWithRetry(sock mangos.Socket, url string) error {
	var (
		retryCount    = 300
		retryInterval = 10
	)
	for {
		err := sock.Listen(url)
		if err == nil {
			context.Log.Infof("plugin start to listen after %d tries", retryCount)
			return err
		}
		retryCount--
		if retryCount < 0 {
			return err
		}
		time.Sleep(time.Duration(retryInterval) * time.Millisecond)
	}
}
