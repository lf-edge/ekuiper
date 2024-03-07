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

package node

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

// JoinAlignNode will block the stream and buffer all the table tuples. Once buffered, it will combine the later input with the buffer
// The input for batch table MUST be *WindowTuples
type JoinAlignNode struct {
	*defaultSinkNode
	// states
	batch map[string][]*xsql.Tuple
}

const BatchKey = "$$batchInputs"

func NewJoinAlignNode(name string, emitters []string, options *api.RuleOption) (*JoinAlignNode, error) {
	batch := make(map[string][]*xsql.Tuple, len(emitters))
	for _, e := range emitters {
		batch[e] = nil
	}
	n := &JoinAlignNode{
		batch: batch,
	}
	n.defaultSinkNode = newDefaultSinkNode(name, options)
	return n, nil
}

func (n *JoinAlignNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	n.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("JoinAlignNode %s is started", n.name)
	n.statManager = metric.NewStatManager(ctx, "op")
	go func() {
		err := infra.SafeRun(func() error {
			// restore batch state
			if s, err := ctx.GetState(BatchKey); err == nil {
				switch st := s.(type) {
				case map[string][]*xsql.Tuple:
					n.batch = st
					log.Infof("Restore batch state %+v", st)
				case nil:
					log.Debugf("Restore batch state, nothing")
				default:
					infra.DrainError(ctx, fmt.Errorf("restore batch state %v error, invalid type", st), errCh)
				}
			} else {
				log.Warnf("Restore batch state fails: %s", err)
			}
			if n.batch == nil {
				n.batch = make(map[string][]*xsql.Tuple)
			}

			for {
				log.Debugf("JoinAlignNode %s is looping", n.name)
				select {
				// process incoming item from both streams(transformed) and tables
				case item, opened := <-n.input:
					processed := false
					if item, processed = n.preprocess(item); processed {
						break
					}
					n.statManager.IncTotalRecordsIn()
					n.statManager.ProcessTimeStart()
					if !opened {
						n.statManager.IncTotalExceptions("input channel closed")
						break
					}
					switch d := item.(type) {
					case error:
						n.Broadcast(d)
						n.statManager.IncTotalExceptions(d.Error())
					case *xsql.WatermarkTuple:
						n.Broadcast(d)
					case *xsql.Tuple:
						log.Debugf("JoinAlignNode receive tuple input %s", d)
						n.alignBatch(ctx, d)
					case *xsql.WindowTuples:
						if d.WindowRange != nil { // real window
							log.Debugf("JoinAlignNode receive window input %s", d)
							n.alignBatch(ctx, d)
						} else { // table window
							log.Debugf("JoinAlignNode receive batch source %s", d)
							if et, ok := d.Content[0].(xsql.EmittedData); ok {
								emitter := et.GetEmitter()
								// Buffer and update batch inputs
								_, ok := n.batch[emitter]
								if !ok {
									e := fmt.Errorf("run JoinAlignNode error: receive batch input from unknown emitter %[1]T(%[1]v)", d)
									n.Broadcast(e)
									n.statManager.IncTotalExceptions(e.Error())
									break
								}
								n.batch[emitter] = convertToTupleSlice(d.Content)
								_ = ctx.PutState(BatchKey, n.batch)
							}
						}
					default:
						e := fmt.Errorf("run JoinAlignNode error: invalid input type but got %[1]T(%[1]v)", d)
						n.Broadcast(e)
						n.statManager.IncTotalExceptions(e.Error())
					}
				case <-ctx.Done():
					log.Infoln("Cancelling join align node....")
					return nil
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func convertToTupleSlice(content []xsql.Row) []*xsql.Tuple {
	tuples := make([]*xsql.Tuple, len(content))
	for i, v := range content {
		tuples[i] = v.(*xsql.Tuple)
	}
	return tuples
}

func (n *JoinAlignNode) alignBatch(_ api.StreamContext, input any) {
	n.statManager.ProcessTimeStart()
	var w *xsql.WindowTuples
	switch t := input.(type) {
	case *xsql.Tuple:
		w = &xsql.WindowTuples{
			Content: make([]xsql.Row, 0),
		}
		w.AddTuple(t)
	case *xsql.WindowTuples:
		w = t
	}
	for _, contents := range n.batch {
		if contents != nil {
			for _, v := range contents {
				w = w.AddTuple(v)
			}
		}
	}
	n.Broadcast(w)
	n.statManager.ProcessTimeEnd()
	n.statManager.IncTotalRecordsOut()
	n.statManager.IncTotalMessagesProcessed(int64(w.Len()))
	n.statManager.SetBufferLength(int64(len(n.input)))
}
