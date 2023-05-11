// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type SwitchConfig struct {
	Cases            []ast.Expr
	StopAtFirstMatch bool
}

type SwitchNode struct {
	*defaultSinkNode
	conf        *SwitchConfig
	statManager metric.StatManager
	outputNodes []defaultNode
}

// GetEmitter returns the nth emitter of the node. SwtichNode is the only node that has multiple emitters
// In planner graph, fromNodes is a multi-dim array, switch node is the only node that could be in the second dim
// The dim is the index
func (n *SwitchNode) GetEmitter(outputIndex int) api.Emitter {
	return &n.outputNodes[outputIndex]
}

// AddOutput SwitchNode overrides the defaultSinkNode's AddOutput to add output to the outputNodes
// SwitchNode itself has multiple outlets defined by the outputNodes.
// This default function will add the output to the first outlet
func (n *SwitchNode) AddOutput(output chan<- interface{}, name string) error {
	if len(n.outputNodes) == 0 { // should never happen
		return fmt.Errorf("no output node is available")
	}
	return n.outputNodes[0].AddOutput(output, name)
}

func NewSwitchNode(name string, conf *SwitchConfig, options *api.RuleOption) (*SwitchNode, error) {
	sn := &SwitchNode{
		conf: conf,
	}
	sn.defaultSinkNode = &defaultSinkNode{
		input: make(chan interface{}, options.BufferLength),
		defaultNode: &defaultNode{
			outputs:   nil,
			name:      name,
			sendError: options.SendError,
		},
	}
	outputs := make([]defaultNode, len(conf.Cases))
	for i := range conf.Cases {
		outputs[i] = defaultNode{
			outputs:   make(map[string]chan<- interface{}),
			name:      name + fmt.Sprintf("_%d", i),
			sendError: options.SendError,
		}
	}
	sn.outputNodes = outputs
	return sn, nil
}

func (n *SwitchNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	ctx.GetLogger().Infof("SwitchNode %s is started", n.name)
	stats, err := metric.NewStatManager(ctx, "op")
	if err != nil {
		infra.DrainError(ctx, fmt.Errorf("cannot create state for switch node %s", n.name), errCh)
		return
	}
	n.statManager = stats
	n.ctx = ctx
	for i := range n.outputNodes {
		n.outputNodes[i].ctx = ctx
	}
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	go func() {
		err := infra.SafeRun(func() error {
			for {
				ctx.GetLogger().Debugf("Switch node %s is looping", n.name)
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
					var ve *xsql.ValuerEval
					switch d := item.(type) {
					case error:
						n.statManager.IncTotalExceptions(d.Error())
						break
					case xsql.TupleRow:
						ctx.GetLogger().Debugf("SwitchNode receive tuple input %s", d)
						ve = &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
					case xsql.SingleCollection:
						ctx.GetLogger().Debugf("SwitchNode receive window input %s", d)
						afv.SetData(d)
						ve = &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(d, fv, d, fv, afv, &xsql.WildcardValuer{Data: d})}
					default:
						e := fmt.Errorf("run switch node error: invalid input type but got %[1]T(%[1]v)", d)
						n.Broadcast(e)
						n.statManager.IncTotalExceptions(e.Error())
						break
					}
				caseLoop:
					for i, c := range n.conf.Cases {
						result := ve.Eval(c)
						switch r := result.(type) {
						case error:
							ctx.GetLogger().Errorf("run switch node %s, case %s error: %s", n.name, c, r)
							n.statManager.IncTotalExceptions(r.Error())
						case bool:
							if r {
								n.outputNodes[i].Broadcast(item)
								if n.conf.StopAtFirstMatch {
									break caseLoop
								}
							}
						case nil: // nil is false
							break
						default:
							m := fmt.Sprintf("run switch node %s, case %s error: invalid condition that returns non-bool value %[1]T(%[1]v)", n.name, c, r)
							ctx.GetLogger().Errorf(m)
							n.statManager.IncTotalExceptions(m)
						}
					}
					n.statManager.ProcessTimeEnd()
					n.statManager.IncTotalRecordsOut()
					n.statManager.SetBufferLength(int64(len(n.input)))
				case <-ctx.Done():
					ctx.GetLogger().Infoln("Cancelling switch node....")
					return nil
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (n *SwitchNode) GetMetrics() [][]interface{} {
	if n.statManager != nil {
		return [][]interface{}{
			n.statManager.GetMetrics(),
		}
	} else {
		return nil
	}
}
