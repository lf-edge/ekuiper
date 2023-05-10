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

package main

import (
	"fmt"

	zmq "github.com/pebbe/zmq4"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type zmqSink struct {
	publisher *zmq.Socket
	srv       string
	topic     string
}

func (m *zmqSink) Configure(props map[string]interface{}) error {
	srv, ok := props["server"]
	if !ok {
		return fmt.Errorf("zmq source is missing property server")
	}
	m.srv, ok = srv.(string)
	if !ok {
		return fmt.Errorf("zmq source property server %v is not a string", srv)
	}
	if tpc, ok := props["topic"]; ok {
		if t, ok := tpc.(string); !ok {
			return fmt.Errorf("zmq source property topic %v is not a string", tpc)
		} else {
			m.topic = t
		}
	}

	m.srv, ok = srv.(string)
	if !ok {
		return fmt.Errorf("zmq source ssing property server")
	}
	return nil
}

func (m *zmqSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	m.publisher, err = zmq.NewSocket(zmq.PUB)
	if err != nil {
		return fmt.Errorf("zmq sink fails to create socket: %v", err)
	}
	err = m.publisher.Bind(m.srv)
	if err != nil {
		return fmt.Errorf("zmq sink fails to bind to %s: %v", m.srv, err)
	}
	logger.Debugf("zmq sink open")
	return nil
}

func (m *zmqSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	var v []byte
	var err error
	v, _, err = ctx.TransformOutput(item, true)
	if err != nil {
		logger.Debug("zmq sink receive non byte data %v", item)
		return err
	}

	logger.Debugf("zmq sink receive %s", item)
	err = m.sendToZmq(ctx, v)
	if err != nil {
		return err
	}

	return nil
}

func (m *zmqSink) sendToZmq(ctx api.StreamContext, v []byte) error {
	var err error
	if m.topic == "" {
		_, err = m.publisher.Send(string(v), 0)
	} else {
		msgs := []string{
			m.topic,
			string(v),
		}
		_, err = m.publisher.SendMessage(msgs)
	}
	if err != nil {
		ctx.GetLogger().Errorf("send to zmq error %v", err)
		return fmt.Errorf("%s:%s", errorx.IOErr, err.Error())
	}
	return nil
}

func (m *zmqSink) Close(ctx api.StreamContext) error {
	if m.publisher != nil {
		return m.publisher.Close()
	}
	return nil
}

func Zmq() api.Sink {
	return &zmqSink{}
}
