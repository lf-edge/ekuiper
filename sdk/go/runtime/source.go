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
	context2 "context"
	"fmt"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/lf-edge/ekuiper/sdk/go/connection"
)

// lifecycle controlled by plugin
// if stop by error, inform plugin

type sourceRuntime struct {
	s      api.Source
	ch     connection.DataOutChannel
	ctx    api.StreamContext
	cancel context2.CancelFunc
	key    string
}

func setupSourceRuntime(con *Control, s api.Source) (*sourceRuntime, error) {
	// init context with args
	ctx, err := parseContext(con)
	// TODO check cmd error handling or using health check
	if err != nil {
		return nil, err
	}
	// init config with args and call source config
	err = s.Configure(con.DataSource, con.Config)
	if err != nil {
		return nil, err
	}
	// connect to mq server
	ch, err := connection.CreateSourceChannel(ctx)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Info("Setup message pipeline, start sending")
	ctx, cancel := ctx.WithCancel()
	return &sourceRuntime{
		s:      s,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
		key:    fmt.Sprintf("%s_%s_%d_%s", con.Meta.RuleId, con.Meta.OpId, con.Meta.InstanceId, con.SymbolName),
	}, nil
}

func (s *sourceRuntime) run() {
	errCh := make(chan error)
	consumer := make(chan api.SourceTuple)
	go s.s.Open(s.ctx, consumer, errCh)
	for {
		select {
		case err := <-errCh:
			s.ctx.GetLogger().Errorf("%v", err)
			broadcast(s.ctx, s.ch, err)
			s.stop()
		case data := <-consumer:
			s.ctx.GetLogger().Debugf("broadcast data %v", data)
			broadcast(s.ctx, s.ch, data)
		case <-s.ctx.Done():
			s.s.Close(s.ctx)
			return
		}
	}
}

func (s *sourceRuntime) stop() error {
	s.cancel()
	err := s.ch.Close()
	if err != nil {
		s.ctx.GetLogger().Info(err)
	}
	s.ctx.GetLogger().Info("closed source data channel")
	reg.Delete(s.key)
	return nil
}

func (s *sourceRuntime) isRunning() bool {
	return s.ctx.Err() == nil
}
