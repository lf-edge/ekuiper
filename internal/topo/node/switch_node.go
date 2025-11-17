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
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type SwitchConfig struct {
	Cases            []ast.Expr
	StopAtFirstMatch bool
}

type SwitchNode struct {
	*defaultSinkNode
	conf        *SwitchConfig
	outputNodes []defaultNode
}

// GetEmitter returns the nth emitter of the node. SwtichNode is the only node that has multiple emitters
// In planner graph, fromNodes is a multi-dim array, switch node is the only node that could be in the second dim
// The dim is the index
func (n *SwitchNode) GetEmitter(outputIndex int) Emitter {
	return &n.outputNodes[outputIndex]
}

// AddOutput SwitchNode overrides the defaultSinkNode's AddOutput to add output to the outputNodes
// SwitchNode itself has multiple outlets defined by the outputNodes.
// This default function will add the output to the first outlet
func (n *SwitchNode) AddOutput(output chan interface{}, name string) error {
	if len(n.outputNodes) == 0 { // should never happen
		return fmt.Errorf("no output node is available")
	}
	return n.outputNodes[0].AddOutput(output, name)
}

func NewSwitchNode(name string, conf *SwitchConfig, options *def.RuleOption) (*SwitchNode, error) {
	sn := &SwitchNode{
		conf: conf,
	}
	sn.defaultSinkNode = newDefaultSinkNode(name, options)
	outputs := make([]defaultNode, len(conf.Cases))
	for i := range conf.Cases {
		outputs[i] = *newDefaultNode(fmt.Sprintf("name_%d", i), options)
	}
	sn.outputNodes = outputs
	return sn, nil
}

func (n *SwitchNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	n.prepareExec(ctx, errCh, "op")
	for i := range n.outputNodes {
		n.outputNodes[i].ctx = ctx
	}
	fv, afv := xsql.NewFunctionValuersForOp(ctx)
	go func() {
		defer func() {
			n.Close()
		}()
		err := infra.SafeRun(func() error {
			for {
				ctx.GetLogger().Debugf("Switch node %s is looping", n.name)
				select {
				// process incoming item from both streams(transformed) and tables
				case item := <-n.input:
					data, processed := n.commonIngest(ctx, item)
					if processed {
						break
					}
					n.onProcessStart(ctx, data)
					var ve *xsql.ValuerEval
					switch d := data.(type) {
					case xsql.Row:
						ctx.GetLogger().Debugf("SwitchNode receive tuple input %s", d)
						ve = &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
					case xsql.Collection:
						ctx.GetLogger().Debugf("SwitchNode receive window input %s", d)
						if cr, ok := d.(xsql.CollectionRow); ok {
							afv.SetData(cr)
							ve = &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(cr, fv, cr, fv, afv, &xsql.WildcardValuer{Data: cr})}
						} else {
							n.onError(ctx, fmt.Errorf("run switch node error: invalid input type but got %[1]T(%[1]v)", d))
							break
						}
					default:
						n.onError(ctx, fmt.Errorf("run switch node error: invalid input type but got %[1]T(%[1]v)", d))
					}
				caseLoop:
					for i, c := range n.conf.Cases {
						result := ve.Eval(c)
						switch r := result.(type) {
						case error:
							n.onError(ctx, r)
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
							n.onError(ctx, fmt.Errorf("run switch node %s, case %s error: invalid condition that returns non-bool value %[1]T(%[1]v)", n.name, c, r))
						}
					}
					n.onProcessEnd(ctx)
					n.statManager.SetBufferLength(int64(len(n.input)))
				case <-ctx.Done():
					ctx.GetLogger().Info("Cancelling switch node....")
					return nil
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}
