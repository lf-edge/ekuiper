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
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/converter"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

// BatchWriterOp is a streaming writer to convert batch data into bytes in streaming way
// Immutable: false
// Input: any (mostly MessageTuple/SinkTupleList, may receive RawTuple after transformOp). Batch EOF is a signal to flush the buffer.
// Output: RawTuple
type BatchWriterOp struct {
	*defaultSinkNode
	writer message.ConvertWriter
	// save lastRow to get the props
	lastRow any
}

func NewBatchWriterOp(ctx api.StreamContext, name string, rOpt *def.RuleOption, sc *SinkConf) (*BatchWriterOp, error) {
	c, err := converter.GetConvertWriter(ctx, sc.Format, sc.SchemaId, nil)
	if err != nil {
		return nil, err
	}
	err = c.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("writer fail to initialize new file: %s", err)
	}
	return &BatchWriterOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		writer:          c,
	}, nil
}

// Exec decode op receives map/[]map and converts it to bytes.
// If receiving bytes, just return it.
func (o *BatchWriterOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			o.Close()
		}()
		err := infra.SafeRun(func() error {
			count := 0
			for {
				select {
				case <-ctx.Done():
					ctx.GetLogger().Infof("watermark node %s is finished", o.name)
					return nil
				case item := <-o.input:
					data, processed := o.ingest(ctx, item)
					if processed {
						break
					}
					switch dt := data.(type) {
					case xsql.BatchEOFTuple:
						if count > 0 {
							// if batch EOF, flush the buffer
							rawBytes, e := o.writer.Flush(ctx)
							if e != nil {
								o.onError(ctx, e)
								break
							}
							// TODO trace for batch
							result := &xsql.RawTuple{Rawdata: rawBytes, Timestamp: time.Time(dt)}
							if ss, ok := o.lastRow.(api.HasDynamicProps); ok {
								result.Props = ss.AllProps()
							}
							o.Broadcast(result)
							o.onSend(ctx, result)
							// sendBatchEnd out raw bytes
							// create a new file
							e = o.writer.New(ctx)
							if e != nil {
								return e
							}
							count = 0
							o.lastRow = nil
						}
					case xsql.Row:
						o.onProcessStart(ctx, data)
						e := o.writer.Write(ctx, dt.ToMap())
						if e != nil {
							o.onError(ctx, e)
						}
						o.onProcessEnd(ctx)
						o.lastRow = dt
						count++
					case xsql.Collection:
						o.onProcessStart(ctx, data)
						e := o.writer.Write(ctx, dt.ToMaps())
						if e != nil {
							o.onError(ctx, e)
						}
						o.onProcessEnd(ctx)
						o.lastRow = dt
						count++
					default:
						o.onError(ctx, fmt.Errorf("unknown data type: %T", data))
					}
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *BatchWriterOp) ingest(ctx api.StreamContext, item any) (any, bool) {
	ctx.GetLogger().Debugf("receive %v", item)
	item, processed := o.preprocess(ctx, item)
	if processed {
		return item, processed
	}
	switch d := item.(type) {
	case error:
		if o.sendError {
			o.Broadcast(d)
		}
		return nil, true
	case *xsql.WatermarkTuple, xsql.EOFTuple:
		o.Broadcast(d)
		return nil, true
	}
	return item, false
}
