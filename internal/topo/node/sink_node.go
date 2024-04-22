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
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SinkNode represents a sink node that collects data from the stream
// It typically only do connect and send. It does not do any processing.
// This node is the skeleton. It will refer to a sink instance to do the real work.
type SinkNode struct {
	*defaultSinkNode
	sink       api.Sink
	eoflimit   int
	currentEof int
	doCollect  func(ctx api.StreamContext, sink api.Sink, data any) error
}

func (s *SinkNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	s.prepareExec(ctx, errCh, "sink")
	go func() {
		err := s.sink.Connect(ctx)
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
		defer s.sink.Close(ctx)
		s.currentEof = 0
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-s.input:
				data, processed := s.ingest(ctx, d)
				if processed {
					break
				}

				s.statManager.IncTotalRecordsIn()
				s.statManager.ProcessTimeStart()
				err = s.doCollect(ctx, s.sink, data)
				if err != nil {
					ctx.GetLogger().Error(err)
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
	ctx.GetLogger().Debugf("%s_%d receive %v", ctx.GetOpId(), ctx.GetInstanceId(), item)
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
		s.currentEof++
		if s.eoflimit == s.currentEof {
			infra.DrainError(ctx, errors.New("done"), s.ctrlCh)
		}
		return nil, true
	}
	ctx.GetLogger().Debugf("%s_%d receive data %v", ctx.GetOpId(), ctx.GetInstanceId(), item)
	return item, false
}

// NewBytesSinkNode creates a sink node that collects data from the stream. Do some static validation
func NewBytesSinkNode(ctx api.StreamContext, name string, sink api.BytesCollector, rOpt *def.RuleOption, eoflimit int) (*SinkNode, error) {
	ctx.GetLogger().Infof("create bytes sink node %s", name)
	return &SinkNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		sink:            sink,
		doCollect:       bytesCollect,
		eoflimit:        eoflimit,
	}, nil
}

func bytesCollect(ctx api.StreamContext, sink api.Sink, data any) (err error) {
	switch d := data.(type) {
	case []byte:
		ctx.GetLogger().Debugf("Sink node %s receive data %s", ctx.GetOpId(), data)
		err = sink.(api.BytesCollector).Collect(ctx, d)
	case error:
		err = sink.(api.BytesCollector).Collect(ctx, []byte(d.Error()))
	default:
		err = fmt.Errorf("expect []byte data type but got %T", d)
	}
	return err
}

// NewTupleSinkNode creates a sink node that collects data from the stream. Do some static validation
func NewTupleSinkNode(ctx api.StreamContext, name string, sink api.TupleCollector, rOpt *def.RuleOption, eoflimit int) (*SinkNode, error) {
	ctx.GetLogger().Infof("create message sink node %s", name)
	return &SinkNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		sink:            sink,
		doCollect:       tupleCollect,
		eoflimit:        eoflimit,
	}, nil
}

// return error that cannot be sent
func tupleCollect(ctx api.StreamContext, sink api.Sink, data any) (err error) {
	switch d := data.(type) {
	case api.Tuple:
		err = sink.(api.TupleCollector).Collect(ctx, d)
	case []api.Tuple:
		err = sink.(api.TupleCollector).CollectList(ctx, d)
	// TODO Make the output all as tuple
	case api.ReadonlyMessage:
		err = sink.(api.TupleCollector).Collect(ctx, model.NewDefaultSourceTuple(d, nil, timex.GetNow()))
	case []api.ReadonlyMessage:
		tuples := make([]api.Tuple, 0, len(d))
		for _, m := range d {
			tuples = append(tuples, model.NewDefaultSourceTuple(m, nil, timex.GetNow()))
		}
		err = sink.(api.TupleCollector).CollectList(ctx, tuples)
	case error:
		err = sink.(api.TupleCollector).Collect(ctx, model.NewDefaultSourceTuple(xsql.Message{"error": d.Error()}, nil, timex.GetNow()))
	default:
		err = fmt.Errorf("expect tuple data type but got %T", d)
	}
	return err
}

var _ DataSinkNode = (*SinkNode)(nil)
