// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/rep"

	// introduce ipc
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
)

// TODO to design timeout strategy

// sockOptions Initialized in config
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
		if err == mangos.ErrProtoState {
			_, err = r.sock.Recv()
			if err == nil {
				err = r.sock.Send(arg)
				if err == nil {
					return nil
				}
			}
		}
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
	t, err := r.sock.GetOption(mangos.OptionRecvDeadline)
	if err != nil {
		return err
	}
	err = r.sock.SetOption(mangos.OptionRecvDeadline, time.Duration(conf.Config.Portable.InitTimeout)*time.Millisecond)
	if err != nil {
		return err
	}
	_, err = r.sock.Recv()
	if err != nil && err != mangos.ErrProtoState {
		return err
	}
	err = r.sock.SetOption(mangos.OptionRecvDeadline, t)
	if err != nil {
		return err
	}
	return nil
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
	conf.Log.Debugf("send request: %s", string(arg))
	for {
		err := r.sock.Send(arg)
		// resend if protocol state wrong, because of plugin restart or other problems
		if err == mangos.ErrProtoState {
			conf.Log.Debugf("send request protestate error %s", err.Error())
			var prev []byte
			prev, err = r.sock.Recv()
			if err == nil {
				conf.Log.Warnf("discard previous response: %s", string(prev))
				conf.Log.Debugf("resend request: %s", string(arg))
				err = r.sock.Send(arg)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("can't send message on function rep socket: %s", err.Error())
		}
		result, e := r.sock.Recv()
		if e != nil {
			conf.Log.Errorf("can't receive: %s", e.Error())
		} else {
			conf.Log.Debugf("receive response: %s", string(result))
		}
		if len(result) > 0 && result[0] == 'h' {
			conf.Log.Debugf("receive handshake response: %s", string(result))
			continue
		}
		return result, e
	}
}

func CreateSourceChannel(ctx api.StreamContext) (DataInChannel, error) {
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
	conf.Log.Infof("source channel created: %s", url)
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
	// Function must send out data quickly and wait for the response with some buffer
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionRecvDeadline: 5000 * time.Millisecond,
		mangos.OptionSendDeadline: 1000 * time.Millisecond,
		mangos.OptionRetryTime:    0,
	})
	url := fmt.Sprintf("ipc:///tmp/func_%s.ipc", symbolName)
	if err = listenWithRetry(sock, url); err != nil {
		return nil, fmt.Errorf("can't listen on rep socket for %s: %s", url, err.Error())
	}
	conf.Log.Infof("function channel created: %s", url)
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
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionSendDeadline: 1000 * time.Millisecond,
	})
	url := fmt.Sprintf("ipc:///tmp/%s_%s_%d.ipc", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	if err = sock.DialOptions(url, dialOptions); err != nil {
		return nil, fmt.Errorf("can't dial on push socket: %s", err.Error())
	}
	conf.Log.Infof("sink channel created: %s", url)
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
	// NO time out now for control channel
	// because the plugin instance liveness can be detected
	// thus, if the plugin exit, the control channel will be closed
	setSockOptions(sock, map[string]interface{}{
		mangos.OptionRecvDeadline: 1 * time.Hour,
	})
	url := fmt.Sprintf("ipc:///tmp/plugin_%s.ipc", pluginName)
	if err = listenWithRetry(sock, url); err != nil {
		return nil, fmt.Errorf("can't listen on rep socket: %s", err.Error())
	}
	conf.Log.Infof("control channel created: %s", url)
	return &NanomsgReqChannel{sock: sock}, nil
}

func setSockOptions(sock mangos.Socket, sockOptions map[string]interface{}) {
	for k, v := range sockOptions {
		err := sock.SetOption(k, v)
		if err != nil && err != mangos.ErrBadOption {
			conf.Log.Errorf("can't set socket option %s: %s", k, err.Error())
		}
	}
}

func listenWithRetry(sock mangos.Socket, url string) error {
	var (
		retryCount    = 5
		retryInterval = 100
	)
	for {
		err := sock.Listen(url)
		if err == nil {
			conf.Log.Infof("start to listen at %s after %d tries", url, 5-retryCount)
			return err
		}
		retryCount--
		if retryCount < 0 {
			return err
		}
		time.Sleep(time.Duration(retryInterval) * time.Millisecond)
	}
}
