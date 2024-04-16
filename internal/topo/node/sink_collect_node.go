// Copyright 2024 EMQ Technologies Co., Ltd.
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

package node

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

// SinkCollectNode represents a sink node that collects data from the stream
// It typically only do connect and send. It does not do any processing.
// This node is the skeleton. It will refer to a sink instance to do the real work.

// BytesSinkNode represents a sink node that collects byte data from the stream
type BytesSinkNode struct {
	*defaultSinkNode
	sink  api.BytesCollector
	errCh chan<- error
}

// NewBytesSinkNode creates a sink node that collects data from the stream. Do some static validation
func NewBytesSinkNode(_ api.StreamContext, name string, sink api.BytesCollector, rOpt *def.RuleOption) (*BytesSinkNode, error) {
	return &BytesSinkNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		sink:            sink,
	}, nil
}

// Exec TODO when to fail?
func (s *BytesSinkNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	s.prepareExec(ctx, errCh, "sink")
	s.errCh = errCh
	go func() {
		err := s.sink.Connect(ctx)
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
		defer s.sink.Close(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-s.input:
				if processed := s.commonIngest(ctx, d); processed {
					break
				}

				s.statManager.IncTotalRecordsIn()
				s.statManager.ProcessTimeStart()
				// TODO send error?
				switch data := d.(type) {
				case []byte:
					ctx.GetLogger().Debugf("Sink node %s receive data %s", s.name, data)
					err = s.sink.Collect(ctx, data)
				default:
					err = fmt.Errorf("expect []byte data type but got %T", d)
				}
				if err != nil {
					s.statManager.IncTotalExceptions(err.Error())
				} else {
					s.statManager.IncTotalRecordsOut()
				}
				s.statManager.ProcessTimeEnd()
				s.statManager.IncTotalMessagesProcessed(1)
			}
		}
	}()
}

// MessageSinkNode represents a sink node that collects message from the stream
type MessageSinkNode struct {
	*defaultSinkNode
	sink  api.TupleCollector
	errCh chan<- error
}

// NewMessageSinkNode creates a sink node that collects data from the stream. Do some static validation
func NewMessageSinkNode(_ api.StreamContext, name string, sink api.TupleCollector, rOpt *def.RuleOption) (*MessageSinkNode, error) {
	return &MessageSinkNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		sink:            sink,
	}, nil
}

// Exec TODO when to fail?
func (s *MessageSinkNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	s.prepareExec(ctx, errCh, "sink")
	go func() {
		err := s.sink.Connect(ctx)
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
		defer s.sink.Close(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-s.input:
				if processed := s.commonIngest(ctx, d); processed {
					break
				}
				s.statManager.IncTotalRecordsIn()
				s.statManager.ProcessTimeStart()
				ctx.GetLogger().Debugf("Sink node %s receive data %s", s.name, d)
				// TODO send error?
				switch data := d.(type) {
				case api.ReadonlyMessage:
					err = s.sink.Collect(ctx, data)
				case []api.ReadonlyMessage:
					err = s.sink.CollectList(ctx, data)
				default:
					err = fmt.Errorf("expect message data type but got %T", d)
				}
				if err != nil {
					s.statManager.IncTotalExceptions(err.Error())
				} else {
					s.statManager.IncTotalRecordsOut()
				}
				s.statManager.ProcessTimeEnd()
				s.statManager.IncTotalMessagesProcessed(1)
			}
		}
	}()
}

var (
	_ DataSinkNode = (*BytesSinkNode)(nil)
	_ DataSinkNode = (*MessageSinkNode)(nil)
)
