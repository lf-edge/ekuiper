// Copyright 2022 EMQ Technologies Co., Ltd.
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

//go:build rpc || !core
// +build rpc !core

package processor

import (
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

func (p *RuleProcessor) ExecQuery(ruleid, sql string) (*topo.Topo, error) {
	if tp, err := planner.PlanSQLWithSourcesAndSinks(p.getDefaultRule(ruleid, sql), nil, []*node.SinkNode{node.NewSinkNode("sink_memory_log", "logToMemory", nil)}); err != nil {
		return nil, err
	} else {
		go func() {
			err := infra.SafeRun(func() error {
				select {
				case err := <-tp.Open():
					if err != nil {
						tp.GetContext().SetError(err)
						tp.Cancel()
						return err
					}
				}
				return nil
			})
			if err != nil {
				log.Infof("closing query for error: %v", err)
			} else {
				log.Info("closing query")
			}
		}()
		return tp, nil
	}
}
