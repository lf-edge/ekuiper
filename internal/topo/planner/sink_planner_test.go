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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestSinkPlan(t *testing.T) {
	tc := []struct {
		name string
		rule *api.Rule
		topo *api.PrintableTopo
	}{
		{
			name: "normal sink plan",
			rule: &api.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{},
					},
				},
				Options: defaultOption,
			},
			topo: &api.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"sink_log_0",
					},
				},
			},
		},
		{
			name: "batch sink plan",
			rule: &api.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"batchSize": 10,
						},
					},
				},
				Options: defaultOption,
			},
			topo: &api.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_log_0_0_batch",
					},
					"op_log_0_0_batch": {
						"sink_log_0",
					},
				},
			},
		},
	}
	for _, c := range tc {
		tp, err := topo.NewWithNameAndOptions("test", c.rule.Options)
		assert.NoError(t, err)
		n := node.NewSourceNode("src1", ast.TypeStream, nil, &ast.Options{
			DATASOURCE: "/feed",
			TYPE:       "httppull",
		}, &api.RuleOption{SendError: false}, false, false, nil)
		tp.AddSrc(n)
		inputs := []api.Emitter{n}
		err = buildActions(tp, c.rule, inputs)
		assert.NoError(t, err)
		assert.Equal(t, c.topo, tp.GetTopo())
	}
}

func TestSinkPlanError(t *testing.T) {
	tc := []struct {
		name string
		rule *api.Rule
		err  string
	}{
		{
			name: "invalid sink",
			rule: &api.Rule{
				Actions: []map[string]any{
					{
						"noexist": map[string]any{},
					},
				},
				Options: defaultOption,
			},
			err: "sink noexist is not defined",
		},
		{
			name: "invalid action format",
			rule: &api.Rule{
				Actions: []map[string]any{
					{
						"log": 12,
					},
				},
				Options: defaultOption,
			},
			err: "expect map[string]interface{} type for the action properties, but found 12",
		},
		{
			name: "invalid batchSize",
			rule: &api.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"batchSize": -1,
						},
					},
				},
				Options: defaultOption,
			},
			err: "fail to parse sink configuration: invalid batchSize -1",
		},
		{
			name: "invalid lingerInterval",
			rule: &api.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"batchSize":      10,
							"lingerInterval": -1,
						},
					},
				},
				Options: defaultOption,
			},
			err: "fail to parse sink configuration: invalid lingerInterval -1",
		},
	}
	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			tp, err := topo.NewWithNameAndOptions("test", c.rule.Options)
			assert.NoError(t, err)
			n := node.NewSourceNode("src1", ast.TypeStream, nil, &ast.Options{
				DATASOURCE: "/feed",
				TYPE:       "httppull",
			}, &api.RuleOption{SendError: false}, false, false, nil)
			tp.AddSrc(n)
			inputs := []api.Emitter{n}
			err = buildActions(tp, c.rule, inputs)
			assert.Error(t, err)
			assert.Equal(t, c.err, err.Error())
		})
	}
}
