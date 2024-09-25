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
	"syscall"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	zmq "github.com/pebbe/zmq4"

	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type zmqSource struct {
	subscriber *zmq.Socket
	zctx       *zmq.Context
	sc         *c
}

func (s *zmqSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	sc, err := validate(ctx, configs)
	if err != nil {
		return err
	}
	s.sc = sc
	return nil
}

func (s *zmqSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	var err error
	defer func() {
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
		} else {
			sch(api.ConnectionConnected, "")
		}
	}()
	// Create a new ZeroMQ context
	zctx, err := zmq.NewContext()
	if err != nil {
		return fmt.Errorf("zmq source fails to create context: %v", err)
	}
	s.zctx = zctx
	s.subscriber, err = zctx.NewSocket(zmq.SUB)
	if err != nil {
		return fmt.Errorf("zmq source fails to create socket: %v", err)
	}
	err = s.subscriber.Connect(s.sc.Server)
	if err != nil {
		return fmt.Errorf("zmq source fails to connect to %s: %v", s.sc.Server, err)
	}
	return nil
}

func (s *zmqSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	ctx.GetLogger().Debugf("zmq source subscribe to topic %s", s.sc.Topic)
	err := s.subscriber.SetSubscribe(s.sc.Topic)
	if err != nil {
		return err
	}
	err = s.subscriber.SetRcvtimeo(time.Second)
	if err != nil {
		return err
	}
	go infra.SafeRun(func() error {
		for {
			msgs, e := s.subscriber.RecvMessageBytes(0)
			if e != nil {
				if zmq.AsErrno(e) == zmq.Errno(syscall.EAGAIN) {
					continue
				}
				id, _ := s.subscriber.GetIdentity()
				ingestError(ctx, fmt.Errorf("zmq source getting message %s error: %v", id, zmq.AsErrno(e)))
			} else {
				rcvTime := timex.GetNow()
				var m []byte
				for i, msg := range msgs {
					if i == 0 && s.sc.Topic != "" {
						continue
					}
					m = append(m, msg...)
				}
				meta := make(map[string]any)
				if s.sc.Topic != "" {
					meta["topic"] = string(msgs[0])
				}
				ingest(ctx, m, meta, rcvTime)
			}
			select {
			case <-ctx.Done():
				s.subscriber.Close()
				s.zctx.Term()
				return nil
			default:
			}
		}
	})
	return nil
}

func (s *zmqSource) Close(_ api.StreamContext) error {
	return nil
}

func GetSource() api.Source {
	return &zmqSource{}
}

var _ api.BytesSource = &zmqSource{}
