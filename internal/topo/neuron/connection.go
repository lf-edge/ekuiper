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
	"sync"
	"sync/atomic"
	"time"
)

const (
	NeuronTopic = "$$neuron"
	NeuronUrl   = "ipc:///tmp/neuron-ekuiper.ipc"
)

var (
	m               sync.RWMutex
	connectionCount int
	sock            mangos.Socket
	opened          int32
	sendTimeout     = 100
)

// createOrGetNeuronConnection creates a new neuron connection or returns an existing one
// This is the entry function for creating a neuron connection singleton
// The context is from a rule, but the singleton will server for multiple rules
func createOrGetConnection(sc api.StreamContext, url string) error {
	m.Lock()
	defer m.Unlock()
	sc.GetLogger().Infof("createOrGetConnection count: %d", connectionCount)
	if connectionCount == 0 {
		sc.GetLogger().Infof("Creating neuron connection")
		contextLogger := conf.Log.WithField("neuron_connection", 0)
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		ruleId := "$$neuron_connection"
		opId := "$$neuron_connection"
		store, err := state.CreateStore(ruleId, 0)
		if err != nil {
			ctx.GetLogger().Errorf("neuron connection create store error %v", err)
			return err
		}
		sctx := ctx.WithMeta(ruleId, opId, store)
		err = connect(sctx, url)
		if err != nil {
			return err
		}
		sc.GetLogger().Infof("Neuron connected")
		pubsub.CreatePub(NeuronTopic)
		go run(sctx)
	}
	connectionCount++
	return nil
}

func closeConnection(ctx api.StreamContext, url string) error {
	m.Lock()
	defer m.Unlock()
	ctx.GetLogger().Infof("closeConnection count: %d", connectionCount)
	pubsub.RemovePub(NeuronTopic)
	if connectionCount == 1 {
		err := disconnect(url)
		if err != nil {
			return err
		}
	}
	connectionCount--
	return nil
}

// nng connections

// connect to nng
func connect(ctx api.StreamContext, url string) error {
	var err error
	sock, err = pair.NewSocket()
	if err != nil {
		return err
	}
	// options consider to export
	err = sock.SetOption(mangos.OptionSendDeadline, time.Duration(sendTimeout)*time.Millisecond)
	if err != nil {
		return err
	}
	sock.SetPipeEventHook(func(ev mangos.PipeEvent, p mangos.Pipe) {
		switch ev {
		case mangos.PipeEventAttached:
			atomic.StoreInt32(&opened, 1)
			conf.Log.Infof("neuron connection attached")
		case mangos.PipeEventAttaching:
			conf.Log.Infof("neuron connection is attaching")
		case mangos.PipeEventDetached:
			atomic.StoreInt32(&opened, 0)
			conf.Log.Warnf("neuron connection detached")
			pubsub.ProduceError(ctx, NeuronTopic, fmt.Errorf("neuron connection detached"))
		}
	})
	//sock.SetOption(mangos.OptionWriteQLen, 100)
	//sock.SetOption(mangos.OptionReadQLen, 100)
	//sock.SetOption(mangos.OptionBestEffort, false)
	if err = sock.DialOptions(url, map[string]interface{}{
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
func run(ctx api.StreamContext) {
	ctx.GetLogger().Infof("neuron source receiving loop started")
	for {
		// no receiving deadline, will wait until the socket closed
		if msg, err := sock.Recv(); err == nil {
			ctx.GetLogger().Debugf("neuron received message %s", string(msg))
			result := make(map[string]interface{})
			err := json.Unmarshal(msg, &result)
			if err != nil {
				ctx.GetLogger().Errorf("neuron decode message error %v", err)
				continue
			}
			pubsub.Produce(ctx, NeuronTopic, result)
		} else if err == mangos.ErrClosed {
			ctx.GetLogger().Infof("neuron connection closed, exit receiving loop")
			return
		} else {
			ctx.GetLogger().Errorf("neuron receiving error %v", err)
		}
	}
}

func publish(ctx api.StreamContext, data []byte) error {
	ctx.GetLogger().Debugf("publish to neuron: %s", string(data))
	if sock != nil && atomic.LoadInt32(&opened) == 1 {
		return sock.Send(data)
	}
	return fmt.Errorf("%s: neuron connection is not established", errorx.IOErr)
}

func disconnect(_ string) error {
	if sock != nil {
		err := sock.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
