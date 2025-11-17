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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

// JoinAlignNode will block the stream and buffer all the table tuples. Once buffered, it will combine the later input with the buffer
// The input for batch table MUST be *WindowTuples
type JoinAlignNode struct {
	*defaultSinkNode
	// table states
	batch map[string][]*xsql.Tuple
	size  map[string]int
}

const BatchKey = "$$batchInputs"

func NewJoinAlignNode(name string, emitters []string, sizes []int, options *def.RuleOption) (*JoinAlignNode, error) {
	batch := make(map[string][]*xsql.Tuple, len(emitters))
	size := make(map[string]int, len(emitters))
	for i, e := range emitters {
		s := sizes[i]
		if s >= 9999 {
			s = 100
		}
		batch[e] = make([]*xsql.Tuple, 0, s)
		size[e] = sizes[i]
	}
	n := &JoinAlignNode{
		batch: batch,
		size:  size,
	}
	n.defaultSinkNode = newDefaultSinkNode(name, options)
	return n, nil
}

func (n *JoinAlignNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	n.prepareExec(ctx, errCh, "op")
	log := ctx.GetLogger()
	go func() {
		defer func() {
			n.Close()
		}()
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
				case item := <-n.input:
					data, processed := n.commonIngest(ctx, item)
					if processed {
						break
					}
					n.onProcessStart(ctx, data)
					switch d := data.(type) {
					case *xsql.Tuple:
						log.Debugf("JoinAlignNode receive tuple input %v", d)
						if b, ok := n.batch[d.Emitter]; ok {
							s := n.size[d.Emitter]
							if len(b) >= s {
								b = b[s-len(b)+1:]
							}
							b = append(b, d)
							n.batch[d.Emitter] = b
							_ = ctx.PutState(BatchKey, n.batch)
						} else {
							n.alignBatch(ctx, d)
						}
					case *xsql.WindowTuples:
						log.Debugf("JoinAlignNode receive window input %v", d)
						n.alignBatch(ctx, d)
					default:
						n.onError(ctx, fmt.Errorf("run JoinAlignNode error: invalid input type but got %[1]T(%[1]v)", d))
					}
					n.onProcessEnd(ctx)
				case <-ctx.Done():
					log.Info("Cancelling join align node....")
					return nil
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (n *JoinAlignNode) alignBatch(ctx api.StreamContext, input any) {
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
		for _, v := range contents {
			w = w.AddTuple(v)
		}
	}
	n.Broadcast(w)
	n.onSend(ctx, w)
	n.statManager.SetBufferLength(int64(len(n.input)))
}
