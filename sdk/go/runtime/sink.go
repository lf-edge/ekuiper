// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	context2 "context"
	"fmt"

	"go.nanomsg.org/mangos/v3"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/lf-edge/ekuiper/sdk/go/connection"
)

type sinkRuntime struct {
	s      api.Sink
	ch     connection.DataInChannel
	ctx    api.StreamContext
	cancel context2.CancelFunc
	key    string
}

func setupSinkRuntime(con *Control, s api.Sink) (*sinkRuntime, error) {
	ctx, err := parseContext(con)
	if err != nil {
		return nil, err
	}
	err = s.Configure(con.Config)
	if err != nil {
		return nil, err
	}
	ch, err := connection.CreateSinkChannel(ctx)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Info("Setup message pipeline, start listening")
	ctx, cancel := ctx.WithCancel()
	return &sinkRuntime{
		s:      s,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
		key:    fmt.Sprintf("%s_%s_%d_%s", con.Meta.RuleId, con.Meta.OpId, con.Meta.InstanceId, con.SymbolName),
	}, nil
}

func (s *sinkRuntime) run() {
	err := s.s.Open(s.ctx)
	if err != nil {
		_ = s.stop()
		return
	}
	for {
		var msg []byte
		// blocking read, must be interrupted when stopping
		// Will be stopped by closing the socket in stop
		msg, err = s.ch.Recv()
		switch err {
		case mangos.ErrClosed:
			break
		case mangos.ErrRecvTimeout:
			continue
		case nil:
			// do nothing
		default:
			s.ctx.GetLogger().Errorf("cannot receive from mangos Socket: %s", err.Error())
			_ = s.stop()
			return
		}
		err = s.s.Collect(s.ctx, msg)
		if err != nil {
			s.ctx.GetLogger().Errorf("collect error: %s", err.Error())
			_ = s.stop()
			return
		}
	}
}

func (s *sinkRuntime) stop() error {
	s.cancel()
	_ = s.s.Close(s.ctx)
	err := s.ch.Close()
	if err != nil {
		s.ctx.GetLogger().Info(err)
	}
	s.ctx.GetLogger().Info("closed sink data channel")
	reg.Delete(s.key)
	return nil
}

func (s *sinkRuntime) isRunning() bool {
	return s.ctx.Err() == nil
}
