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

package neuron

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TopicPrefix      = "$$neuron_"
	DefaultNeuronUrl = "ipc:///tmp/neuron-ekuiper.ipc"
)

type conninfo struct {
	count  int
	sock   mangos.Socket
	opened int32
}

var (
	m             sync.RWMutex
	connectionReg = make(map[string]*conninfo)
	sendTimeout   = 100
)

// createOrGetNeuronConnection creates a new neuron connection or returns an existing one
// This is the entry function for creating a neuron connection singleton
// The context is from a rule, but the singleton will server for multiple rules
func createOrGetConnection(sc api.StreamContext, url string) (*conninfo, error) {
	m.Lock()
	defer m.Unlock()
	sc.GetLogger().Infof("createOrGetConnection for %s", url)
	info, ok := connectionReg[url]
	if !ok || info.count <= 0 {
		sc.GetLogger().Infof("Creating neuron connection for %s", url)
		contextLogger := conf.Log.WithField("neuron_connection_url", url)
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		ruleId := "$$neuron_connection_" + url
		opId := "$$neuron_connection_" + url
		store, err := state.CreateStore(ruleId, 0)
		if err != nil {
			ctx.GetLogger().Errorf("neuron connection create store error %v", err)
			return nil, err
		}
		sctx := ctx.WithMeta(ruleId, opId, store)
		info = &conninfo{count: 0}
		connectionReg[url] = info
		err = connect(sctx, url, info)
		if err != nil {
			return nil, err
		}
		sc.GetLogger().Infof("Neuron %s connected", url)
		pubsub.CreatePub(TopicPrefix + url)
		go run(sctx, info, url)
	}
	info.count++
	return info, nil
}

func closeConnection(ctx api.StreamContext, url string) error {
	m.Lock()
	defer m.Unlock()
	ctx.GetLogger().Infof("closeConnection %s", url)
	info, ok := connectionReg[url]
	if !ok {
		return fmt.Errorf("no connection for %s", url)
	}
	pubsub.RemovePub(TopicPrefix + url)
	if info.count == 1 {
		if info.sock != nil {
			err := info.sock.Close()
			if err != nil {
				return err
			}
		}
	}
	info.count--
	return nil
}

// nng connections

// connect to nng
func connect(ctx api.StreamContext, url string, info *conninfo) error {
	var err error
	info.sock, err = pair.NewSocket()
	if err != nil {
		return err
	}
	// options consider to export
	err = info.sock.SetOption(mangos.OptionSendDeadline, time.Duration(sendTimeout)*time.Millisecond)
	if err != nil {
		return err
	}
	info.sock.SetPipeEventHook(func(ev mangos.PipeEvent, p mangos.Pipe) {
		switch ev {
		case mangos.PipeEventAttached:
			atomic.StoreInt32(&info.opened, 1)
			conf.Log.Infof("neuron connection attached")
		case mangos.PipeEventAttaching:
			conf.Log.Infof("neuron connection is attaching")
		case mangos.PipeEventDetached:
			atomic.StoreInt32(&info.opened, 0)
			conf.Log.Warnf("neuron connection detached")
			pubsub.ProduceError(ctx, TopicPrefix+url, fmt.Errorf("neuron connection detached"))
		}
	})
	//sock.SetOption(mangos.OptionWriteQLen, 100)
	//sock.SetOption(mangos.OptionReadQLen, 100)
	//sock.SetOption(mangos.OptionBestEffort, false)
	if err = info.sock.DialOptions(url, map[string]interface{}{
		mangos.OptionDialAsynch:       true, // will not report error and keep connecting
		mangos.OptionMaxReconnectTime: 5 * time.Second,
		mangos.OptionReconnectTime:    100 * time.Millisecond,
	}); err != nil {
		return fmt.Errorf("please make sure neuron has started and configured, can't dial to neuron: %s", err.Error())
	}

	return nil
}

// run the loop to receive message from the nng connection singleton
// exit when connection is closed
func run(ctx api.StreamContext, info *conninfo, url string) {
	ctx.GetLogger().Infof("neuron source receiving loop started")
	for {
		// no receiving deadline, will wait until the socket closed
		if msg, err := info.sock.Recv(); err == nil {
			ctx.GetLogger().Debugf("neuron received message %s", string(msg))
			result := make(map[string]interface{})
			err := json.Unmarshal(msg, &result)
			if err != nil {
				ctx.GetLogger().Errorf("neuron decode message error %v", err)
				continue
			}
			pubsub.Produce(ctx, TopicPrefix+url, result)
		} else if err == mangos.ErrClosed {
			ctx.GetLogger().Infof("neuron connection closed, exit receiving loop")
			return
		} else {
			ctx.GetLogger().Errorf("neuron receiving error %v", err)
		}
	}
}

func publish(ctx api.StreamContext, data []byte, info *conninfo) error {
	ctx.GetLogger().Debugf("publish to neuron: %s", string(data))
	if info.sock != nil && atomic.LoadInt32(&info.opened) == 1 {
		return info.sock.Send(data)
	}
	return fmt.Errorf("%s: neuron connection is not established", errorx.IOErr)
}
