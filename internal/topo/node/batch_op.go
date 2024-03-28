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

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type BatchOp struct {
	*defaultSinkNode
	// configs
	batchSize      int
	lingerInterval int
	// state
	buffer    *xsql.WindowTuples
	currIndex int
}

func NewBatchOp(name string, rOpt *api.RuleOption, batchSize, lingerInterval int) (*BatchOp, error) {
	if batchSize < 1 && lingerInterval < 1 {
		return nil, fmt.Errorf("either batchSize or lingerInterval should be larger than 0")
	}
	o := &BatchOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		batchSize:       batchSize,
		lingerInterval:  lingerInterval,
		currIndex:       0,
	}
	if batchSize == 0 {
		batchSize = 1024
	}
	o.buffer = &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, batchSize),
	}
	return o, nil
}

func (b *BatchOp) Exec(ctx api.StreamContext, _ chan<- error) {
	ctx.GetLogger().Infof("batch op started")
	b.statManager = metric.NewStatManager(ctx, "op")
	b.ctx = ctx
	switch {
	case b.batchSize > 0 && b.lingerInterval > 0:
		b.runWithTickerAndBatchSize(ctx)
	case b.batchSize > 0 && b.lingerInterval == 0:
		b.runWithBatchSize(ctx)
	case b.batchSize == 0 && b.lingerInterval > 0:
		b.runWithTicker(ctx)
	}
}

func (b *BatchOp) runWithTickerAndBatchSize(ctx api.StreamContext) {
	ticker := conf.GetTicker(int64(b.lingerInterval))
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-b.input:
				b.ingest(ctx, d, true)
			case <-ticker.C:
				b.send()
			}
		}
	}()
}

func (b *BatchOp) ingest(ctx api.StreamContext, item any, checkSize bool) {
	ctx.GetLogger().Debugf("batch op receive %v", item)
	processed := false
	if item, processed = b.preprocess(item); processed {
		return
	}
	switch d := item.(type) {
	case error:
		b.Broadcast(d)
		b.statManager.IncTotalExceptions(d.Error())
		return
	case *xsql.WatermarkTuple:
		b.Broadcast(d)
		return
	}

	b.statManager.IncTotalRecordsIn()
	b.statManager.ProcessTimeStart()
	switch input := item.(type) {
	case xsql.Row:
		b.buffer.AddTuple(input)
	case xsql.Collection:
		_ = input.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			b.buffer.AddTuple(r.(xsql.Row))
			return true, nil
		})
	default:
		ctx.GetLogger().Errorf("run batch error: invalid data type %T", input)
	}
	b.currIndex++
	if checkSize && b.currIndex >= b.batchSize {
		b.send()
		b.statManager.IncTotalRecordsOut()
	}
	b.statManager.ProcessTimeEnd()
	b.statManager.IncTotalMessagesProcessed(1)
	b.statManager.SetBufferLength(int64(len(b.input) + b.currIndex))
}

func (b *BatchOp) send() {
	b.Broadcast(b.buffer)
	// Reset buffer
	b.buffer = &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, b.batchSize),
	}
	b.currIndex = 0
}

func (b *BatchOp) runWithBatchSize(ctx api.StreamContext) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-b.input:
				b.ingest(ctx, d, true)
			}
		}
	}()
}

func (b *BatchOp) runWithTicker(ctx api.StreamContext) {
	ticker := conf.GetTicker(int64(b.lingerInterval))
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-b.input:
				b.ingest(ctx, d, false)
			case <-ticker.C:
				b.send()
			}
		}
	}()
}
