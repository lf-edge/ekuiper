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
	statManager metric.StatManager
	// states
	batch map[string][]xsql.TupleRow
}

const BatchKey = "$$batchInputs"

func NewJoinAlignNode(name string, emitters []string, options *api.RuleOption) (*JoinAlignNode, error) {
	batch := make(map[string][]xsql.TupleRow, len(emitters))
	for _, e := range emitters {
		batch[e] = nil
	}
	n := &JoinAlignNode{
		batch: batch,
	}
	n.defaultSinkNode = &defaultSinkNode{
		input: make(chan interface{}, options.BufferLength),
		defaultNode: &defaultNode{
			outputs:   make(map[string]chan<- interface{}),
			name:      name,
			sendError: options.SendError,
		},
	}
	return n, nil
}

func (n *JoinAlignNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	n.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("JoinAlignNode %s is started", n.name)

	if len(n.outputs) <= 0 {
		infra.DrainError(ctx, fmt.Errorf("no output channel found"), errCh)
		return
	}
	stats, err := metric.NewStatManager(ctx, "op")
	if err != nil {
		infra.DrainError(ctx, fmt.Errorf("no output channel found"), errCh)
		return
	}
	n.statManager = stats
	go func() {
		err := infra.SafeRun(func() error {
			// restore batch state
			if s, err := ctx.GetState(BatchKey); err == nil {
				switch st := s.(type) {
				case map[string][]xsql.TupleRow:
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
				n.batch = make(map[string][]xsql.TupleRow)
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
					case *xsql.Tuple:
						log.Debugf("JoinAlignNode receive tuple input %s", d)
						temp := &xsql.WindowTuples{
							Content: make([]xsql.TupleRow, 0),
						}
						temp = temp.AddTuple(d)
						n.alignBatch(ctx, temp)
					case *xsql.WindowTuples:
						if d.WindowRange != nil { // real window
							log.Debugf("JoinAlignNode receive window input %s", d)
							n.alignBatch(ctx, d)
						} else { // table window
							log.Debugf("JoinAlignNode receive batch source %s", d)
							emitter := d.Content[0].GetEmitter()
							// Buffer and update batch inputs
							_, ok := n.batch[emitter]
							if !ok {
								e := fmt.Errorf("run JoinAlignNode error: receive batch input from unknown emitter %[1]T(%[1]v)", d)
								n.Broadcast(e)
								n.statManager.IncTotalExceptions(e.Error())
								break
							} else {
								n.batch[emitter] = d.Content
								ctx.PutState(BatchKey, n.batch)
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

func (n *JoinAlignNode) alignBatch(_ api.StreamContext, w *xsql.WindowTuples) {
	n.statManager.ProcessTimeStart()
	for _, v := range n.batch {
		if v != nil {
			w.Content = append(w.Content, v...)
		}
	}

	n.Broadcast(w)
	n.statManager.ProcessTimeEnd()
	n.statManager.IncTotalRecordsOut()
	n.statManager.SetBufferLength(int64(len(n.input)))
}

func (n *JoinAlignNode) GetMetrics() [][]interface{} {
	if n.statManager != nil {
		return [][]interface{}{
			n.statManager.GetMetrics(),
		}
	} else {
		return nil
	}
}
