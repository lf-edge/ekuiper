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

//go:build !windows

package zmq

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	zmq "github.com/pebbe/zmq4"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type zmqSink struct {
	publisher *zmq.Socket
	sc        *c
}

func (m *zmqSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	sc, err := validate(ctx, configs)
	if err != nil {
		return err
	}
	m.sc = sc
	return nil
}

func (m *zmqSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) (err error) {
	defer func() {
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
		} else {
			sch(api.ConnectionConnected, "")
		}
	}()
	m.publisher, err = zmq.NewSocket(zmq.PUB)
	if err != nil {
		return fmt.Errorf("zmq sink fails to create socket: %v", err)
	}
	err = m.publisher.Bind(m.sc.Server)
	if err != nil {
		return fmt.Errorf("zmq sink fails to bind to %s: %v", m.sc.Server, err)
	}
	ctx.GetLogger().Debugf("zmq sink open")
	return nil
}

func (m *zmqSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	return m.sendToZmq(ctx, item.Raw())
}

func (m *zmqSink) sendToZmq(ctx api.StreamContext, v []byte) error {
	var err error
	if m.sc.Topic2 == "" {
		_, err = m.publisher.SendBytes(v, 0)
	} else {
		msgs := [][]byte{
			[]byte(m.sc.Topic),
			v,
		}
		_, err = m.publisher.SendMessage(msgs)
	}
	if err != nil {
		ctx.GetLogger().Errorf("send to zmq error %v", err)
		return errorx.NewIOErr(err.Error())
	}
	return nil
}

func (m *zmqSink) Close(_ api.StreamContext) error {
	if m.publisher != nil {
		return m.publisher.Close()
	}
	return nil
}

func GetSink() api.Sink {
	return &zmqSink{}
}

var _ api.BytesCollector = &zmqSink{}
