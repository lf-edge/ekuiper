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
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

// LookupNode will look up the data from the external source when receiving an event
type LookupNode struct {
	*defaultSinkNode
	statManager metric.StatManager
	sourceType  string
	joinType    ast.JoinType
	vals        []ast.Expr

	srcOptions *ast.Options
	Keys       []string
}

func NewLookupNode(name string, keys []string, joinType ast.JoinType, vals []ast.Expr, srcOptions *ast.Options, options *api.RuleOption) (*LookupNode, error) {
	t := srcOptions.TYPE
	if t == "" {
		return nil, fmt.Errorf("source type is not specified")
	}
	n := &LookupNode{
		Keys:       keys,
		srcOptions: srcOptions,
		sourceType: t,
		joinType:   joinType,
		vals:       vals,
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

func (n *LookupNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	n.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("LookupNode %s is started", n.name)

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
			props := getSourceConf(ctx, n.sourceType, n.srcOptions)
			ctx.GetLogger().Infof("open lookup source node with props %v", conf.Printable(props))
			// Create the lookup source according to the source options
			ns, err := io.LookupSource(n.sourceType)
			if err != nil {
				return err
			}
			err = ns.Configure(n.srcOptions.DATASOURCE, props, n.Keys)
			if err != nil {
				return err
			}
			err = ns.Open(ctx)
			if err != nil {
				return err
			}
			fv, _ := xsql.NewFunctionValuersForOp(ctx)
			// Start the lookup source loop
			for {
				log.Debugf("LookupNode %s is looping", n.name)
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
					case xsql.TupleRow:
						log.Debugf("Lookup Node receive tuple input %s", d)
						n.statManager.ProcessTimeStart()
						sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}
						err := n.lookup(ctx, d, fv, ns, sets)
						if err != nil {
							n.Broadcast(err)
							n.statManager.IncTotalExceptions(err.Error())
						} else {
							n.Broadcast(sets)
							n.statManager.IncTotalRecordsOut()
						}
						n.statManager.ProcessTimeEnd()
						n.statManager.SetBufferLength(int64(len(n.input)))
					case *xsql.WindowTuples:
						log.Debugf("Lookup Node receive window input %s", d)
						n.statManager.ProcessTimeStart()
						sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}
						err := d.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
							tr, ok := r.(xsql.TupleRow)
							if !ok {
								return false, fmt.Errorf("Invalid window element, must be a tuple row but got %v", r)
							}
							err := n.lookup(ctx, tr, fv, ns, sets)
							if err != nil {
								return false, err
							}
							return true, nil
						})
						if err != nil {
							n.Broadcast(err)
							n.statManager.IncTotalExceptions(err.Error())
						} else {
							n.Broadcast(sets)
							n.statManager.IncTotalRecordsOut()
						}
						n.statManager.ProcessTimeEnd()
						n.statManager.SetBufferLength(int64(len(n.input)))
					default:
						e := fmt.Errorf("run lookup node error: invalid input type but got %[1]T(%[1]v)", d)
						n.Broadcast(e)
						n.statManager.IncTotalExceptions(e.Error())
					}
				case <-ctx.Done():
					log.Infoln("Cancelling lookup node....")
					return nil
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (n *LookupNode) lookup(ctx api.StreamContext, d xsql.TupleRow, fv *xsql.FunctionValuer, ns api.LookupSource, tuples *xsql.JoinTuples) error {
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
	cvs := make([]interface{}, len(n.vals))
	for i, val := range n.vals {
		cvs[i] = ve.Eval(val)
	}
	r, e := ns.Lookup(ctx, cvs)
	if e != nil {
		return e
	} else {
		if len(r) == 0 {
			if n.joinType == ast.LEFT_JOIN {
				merged := &xsql.JoinTuple{}
				merged.AddTuple(d)
				tuples.Content = append(tuples.Content, merged)
			} else {
				ctx.GetLogger().Debugf("Lookup Node %s no result found for tuple %s", n.name, d)
				return nil
			}
		}
		for _, v := range r {
			merged := &xsql.JoinTuple{}
			merged.AddTuple(d)
			t := &xsql.Tuple{
				Emitter:   n.name,
				Message:   v.Message(),
				Metadata:  v.Meta(),
				Timestamp: conf.GetNowInMilli(),
			}
			merged.AddTuple(t)
			tuples.Content = append(tuples.Content, merged)
		}
		return nil
	}
}

func (n *LookupNode) GetMetrics() [][]interface{} {
	if n.statManager != nil {
		return [][]interface{}{
			n.statManager.GetMetrics(),
		}
	} else {
		return nil
	}
}

func (n *LookupNode) merge(ctx api.StreamContext, d xsql.TupleRow, r []map[string]interface{}) {
	n.statManager.ProcessTimeStart()
	sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}

	if len(r) == 0 {
		if n.joinType == ast.LEFT_JOIN {
			merged := &xsql.JoinTuple{}
			merged.AddTuple(d)
			sets.Content = append(sets.Content, merged)
		} else {
			ctx.GetLogger().Debugf("Lookup Node %s no result found for tuple %s", n.name, d)
			return
		}
	}
	for _, v := range r {
		merged := &xsql.JoinTuple{}
		merged.AddTuple(d)
		t := &xsql.Tuple{
			Emitter:   n.name,
			Message:   v,
			Timestamp: conf.GetNowInMilli(),
		}
		merged.AddTuple(t)
		sets.Content = append(sets.Content, merged)
	}

	n.Broadcast(sets)
	n.statManager.ProcessTimeEnd()
	n.statManager.IncTotalRecordsOut()
	n.statManager.SetBufferLength(int64(len(n.input)))
}
