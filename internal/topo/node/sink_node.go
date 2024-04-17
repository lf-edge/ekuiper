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
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SinkNode represents a sink node that collects data from the stream
// It typically only do connect and send. It does not do any processing.
// This node is the skeleton. It will refer to a sink instance to do the real work.
type SinkNode struct {
	*defaultSinkNode
	sink      api.Sink
	doCollect func(ctx api.StreamContext, sink api.Sink, data any) error
}

func (s *SinkNode) Exec(ctx api.StreamContext, errCh chan<- error) {
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
				data, processed := s.ingest(ctx, d)
				if processed {
					return
				}

				s.statManager.IncTotalRecordsIn()
				s.statManager.ProcessTimeStart()
				err = s.doCollect(ctx, s.sink, data)
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

func (s *SinkNode) ingest(ctx api.StreamContext, item any) (any, bool) {
	ctx.GetLogger().Debugf("receive %v", item)
	item, processed := s.preprocess(ctx, item)
	if processed {
		return item, processed
	}
	switch d := item.(type) {
	case error:
		s.statManager.IncTotalExceptions(d.Error())
		if s.sendError {
			return d, false
		}
		return nil, true
	case *xsql.WatermarkTuple:
		return nil, true
	case xsql.EOFTuple:
		infra.DrainError(ctx, errors.New("done"), s.ctrlCh)
		return nil, true
	}
	return item, false
}

// NewBytesSinkNode creates a sink node that collects data from the stream. Do some static validation
func NewBytesSinkNode(ctx api.StreamContext, name string, sink api.BytesCollector, rOpt *def.RuleOption) (*SinkNode, error) {
	ctx.GetLogger().Infof("create bytes sink node %s", name)
	return &SinkNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		sink:            sink,
		doCollect:       bytesCollect,
	}, nil
}

func bytesCollect(ctx api.StreamContext, sink api.Sink, data any) (err error) {
	switch d := data.(type) {
	case []byte:
		ctx.GetLogger().Debugf("Sink node %s receive data %s", ctx.GetOpId(), data)
		err = sink.(api.BytesCollector).Collect(ctx, d)
	default:
		err = fmt.Errorf("expect []byte data type but got %T", d)
	}
	return err
}

// NewTupleSinkNode creates a sink node that collects data from the stream. Do some static validation
func NewTupleSinkNode(ctx api.StreamContext, name string, sink api.TupleCollector, rOpt *def.RuleOption) (*SinkNode, error) {
	ctx.GetLogger().Infof("create message sink node %s", name)
	return &SinkNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		sink:            sink,
		doCollect:       tupleCollect,
	}, nil
}

func tupleCollect(ctx api.StreamContext, sink api.Sink, data any) (err error) {
	switch d := data.(type) {
	case api.Tuple:
		err = sink.(api.TupleCollector).Collect(ctx, d)
	case []api.Tuple:
		err = sink.(api.TupleCollector).CollectList(ctx, d)
	// TODO Make the output all as tuple
	case api.ReadonlyMessage:
		err = sink.(api.TupleCollector).Collect(ctx, api.NewDefaultSourceTuple(d, nil, timex.GetNow()))
	case []api.ReadonlyMessage:
		tuples := make([]api.Tuple, 0, len(d))
		for _, m := range d {
			tuples = append(tuples, api.NewDefaultSourceTuple(m, nil, timex.GetNow()))
		}
		err = sink.(api.TupleCollector).CollectList(ctx, tuples)
	default:
		err = fmt.Errorf("expect message data type but got %T", d)
	}
	return err
}

var _ DataSinkNode = (*SinkNode)(nil)
