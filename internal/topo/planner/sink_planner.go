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

package planner

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// SinkPlanner is the planner for sink node. It transforms logical sink plan to multiple physical nodes.
// It will split the sink plan into multiple sink nodes according to its sink configurations.

func buildActions(tp *topo.Topo, rule *api.Rule, inputs []api.Emitter) error {
	for i, m := range rule.Actions {
		for name, action := range m {
			s, _ := io.Sink(name)
			if s == nil {
				return fmt.Errorf("sink %s is not defined", name)
			}
			props, ok := action.(map[string]any)
			if !ok {
				return fmt.Errorf("expect map[string]interface{} type for the action properties, but found %v", action)
			}
			commonConf, err := node.ParseConf(conf.Log, props)
			if err != nil {
				return fmt.Errorf("fail to parse sink configuration: %v", err)
			}
			// Split sink node
			sinkName := fmt.Sprintf("%s_%d", name, i)
			newInputs, err := splitSink(tp, inputs, sinkName, rule.Options, commonConf)
			if err != nil {
				return err
			}
			if s != nil {
				if err = s.Configure(props); err != nil {
					return err
				}
			}
			tp.AddSink(newInputs, node.NewSinkNode(sinkName, name, props))
		}
	}
	return nil
}

// Split sink node according to the sink configuration. Return the new input emitters.
func splitSink(tp *topo.Topo, inputs []api.Emitter, sinkName string, options *api.RuleOption, sc *node.SinkConf) ([]api.Emitter, error) {
	index := 0
	newInputs := inputs
	// Batch enabled
	if sc.BatchSize > 0 || sc.LingerInterval > 0 {
		batchOp, err := node.NewBatchOp(fmt.Sprintf("%s_%d_batch", sinkName, index), options, sc.BatchSize, sc.LingerInterval)
		if err != nil {
			return nil, err
		}
		index++
		tp.AddOperator(newInputs, batchOp)
		newInputs = []api.Emitter{batchOp}
	}
	return newInputs, nil
}
