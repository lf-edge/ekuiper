// Copyright 2023 EMQ Technologies Co., Ltd.
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

type ProjectSetNode struct {
	*defaultSinkNode
	SrfMapping map[string]struct{}
}

func NewProjectSetNode(name string, options *api.RuleOption, srfMapping map[string]struct{}) *ProjectSetNode {
	node := &ProjectSetNode{}
	node.defaultSinkNode = &defaultSinkNode{
		input: make(chan interface{}, options.BufferLength),
		defaultNode: &defaultNode{
			outputs:   make(map[string]chan<- interface{}),
			name:      name,
			sendError: options.SendError,
		},
	}
	node.SrfMapping = srfMapping
	return node
}

func (node *ProjectSetNode) apply(data interface{}) []interface{} {
	// for now we only support 1 srf function in the field
	srfName := ""
	for k := range node.SrfMapping {
		srfName = k
		break
	}
	switch input := data.(type) {
	case error:
		return []interface{}{input}
	case xsql.TupleRow:
		aValue, ok := input.Value(srfName, "")
		if !ok {
			return []interface{}{fmt.Errorf("can't find the result from the %v function", srfName)}
		}
		aValues, ok := aValue.([]interface{})
		if !ok {
			return []interface{}{fmt.Errorf("the result from the %v function should be array", srfName)}
		}
		newData := make([]interface{}, 0)
		for _, v := range aValues {
			newTupleRow := input.Clone()
			// clear original column value
			newTupleRow.Del(srfName)
			if mv, ok := v.(map[string]interface{}); ok {
				for k, v := range mv {
					newTupleRow.Set(k, v)
				}
			} else {
				newTupleRow.Set(srfName, v)
			}
			newData = append(newData, newTupleRow)
		}
		return newData
	default:
		return []interface{}{fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)}
	}
}

func (node *ProjectSetNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	node.ctx = ctx
	logger := ctx.GetLogger()
	defer func() {
		logger.Infof("ProjectSetNode %s instance %d done, cancelling future items", node.name, ctx.GetInstanceId())
	}()
	stats, err := metric.NewStatManager(ctx, "op")
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	node.statManagers = append(node.statManagers, stats)
	go func() {
		err := infra.SafeRun(func() error {
			for {
				select {
				// process incoming item
				case item := <-node.input:
					processed := false
					if item, processed = node.preprocess(item); processed {
						break
					}
					stats.IncTotalRecordsIn()
					stats.ProcessTimeStart()
					results := node.apply(item)
					if results == nil || len(results) == 0 {
						continue
					}
					for i, result := range results {
						switch val := result.(type) {
						case nil:
							continue
						case error:
							logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val)
							node.Broadcast(val)
							stats.IncTotalExceptions(val.Error())
							continue
						default:
							if i == 0 {
								stats.ProcessTimeEnd()
							}
							node.Broadcast(val)
							stats.IncTotalRecordsOut()
						}
					}
					stats.SetBufferLength(int64(len(node.input)))
				// is cancelling
				case <-ctx.Done():
					logger.Infof("ProjectSetNode %s instance %d cancelling....", node.name, ctx.GetInstanceId())
					return nil
				}
			}
		})
		if err != nil {
			errCh <- err
		}
	}()
}
