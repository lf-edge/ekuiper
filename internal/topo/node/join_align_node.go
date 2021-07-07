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

package node

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// JoinAlignNode will block the stream and buffer all the table tuples. Once buffered, it will combine the later input with the buffer
// The input for batch table MUST be *WindowTuples
type JoinAlignNode struct {
	*defaultSinkNode
	statManager StatManager
	emitters    map[string]int
	// states
	batch *xsql.WindowTuplesSet
}

const BatchKey = "$$batchInputs"

func NewJoinAlignNode(name string, emitters []string, options *api.RuleOption) (*JoinAlignNode, error) {
	emap := make(map[string]int, len(emitters))
	for i, e := range emitters {
		emap[e] = i
	}
	n := &JoinAlignNode{
		emitters: emap,
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
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}
	stats, err := NewStatManager("op", ctx)
	if err != nil {
		go func() { errCh <- err }()
		return
	}
	n.statManager = stats
	go func() {
		// restore batch state
		if s, err := ctx.GetState(BatchKey); err == nil {
			switch st := s.(type) {
			case []xsql.WindowTuples:
				if len(st) == len(n.emitters) {
					n.batch = &xsql.WindowTuplesSet{Content: st}
					log.Infof("Restore batch state %+v", st)
				} else {
					log.Warnf("Restore batch state got different emitter length so discarded: %+v", st)
				}
			case nil:
				log.Debugf("Restore batch state, nothing")
			default:
				errCh <- fmt.Errorf("restore batch state %v error, invalid type", st)
			}
		} else {
			log.Warnf("Restore batch state fails: %s", err)
		}
		if n.batch == nil {
			n.batch = &xsql.WindowTuplesSet{
				Content: make([]xsql.WindowTuples, len(n.emitters)),
			}
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
					n.statManager.IncTotalExceptions()
					break
				}
				switch d := item.(type) {
				case error:
					n.Broadcast(d)
					n.statManager.IncTotalExceptions()
				case *xsql.Tuple:
					log.Debugf("JoinAlignNode receive tuple input %s", d)
					temp := xsql.WindowTuplesSet{
						Content: make([]xsql.WindowTuples, 0),
					}
					temp = temp.AddTuple(d)
					n.alignBatch(ctx, temp)
				case xsql.WindowTuplesSet:
					log.Debugf("JoinAlignNode receive window input %s", d)
					n.alignBatch(ctx, d)
				case xsql.WindowTuples: // batch input
					log.Debugf("JoinAlignNode receive batch source %s", d)
					// Buffer and update batch inputs
					index, ok := n.emitters[d.Emitter]
					if !ok {
						n.Broadcast(fmt.Errorf("run JoinAlignNode error: receive batch input from unknown emitter %[1]T(%[1]v)", d))
						n.statManager.IncTotalExceptions()
					}
					if n.batch != nil && len(n.batch.Content) > index {
						n.batch.Content[index] = d
						ctx.PutState(BatchKey, n.batch)
					} else {
						log.Errorf("Invalid index %d for batch %v", index, n.batch)
					}
				default:
					n.Broadcast(fmt.Errorf("run JoinAlignNode error: invalid input type but got %[1]T(%[1]v)", d))
					n.statManager.IncTotalExceptions()
				}
			case <-ctx.Done():
				log.Infoln("Cancelling join align node....")
				return
			}
		}
	}()
}

func (n *JoinAlignNode) alignBatch(_ api.StreamContext, w xsql.WindowTuplesSet) {
	n.statManager.ProcessTimeStart()
	w.Content = append(w.Content, n.batch.Content...)
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
