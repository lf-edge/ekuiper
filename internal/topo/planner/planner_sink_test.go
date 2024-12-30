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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
)

func TestSinkPlan(t *testing.T) {
	tc := []struct {
		name string
		rule *def.Rule
		topo *def.PrintableTopo
	}{
		{
			name: "normal sink plan",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_log_0_0_transform",
					},
					"op_log_0_0_transform": {
						"op_log_0_1_encode",
					},
					"op_log_0_1_encode": {
						"sink_log_0",
					},
				},
			},
		},
		{
			name: "batch sink plan",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"batchSize": 10,
						},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_log_0_0_batch",
					},
					"op_log_0_0_batch": {
						"op_log_0_1_transform",
					},
					"op_log_0_1_transform": {
						"op_log_0_2_batchWriter",
					},
					"op_log_0_2_batchWriter": {
						"sink_log_0",
					},
				},
			},
		},
		{
			name: "batch and compress sink plan",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"batchSize":   10,
							"compression": "gzip",
						},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_log_0_0_batch",
					},
					"op_log_0_0_batch": {
						"op_log_0_1_transform",
					},
					"op_log_0_1_transform": {
						"op_log_0_2_batchWriter",
					},
					"op_log_0_2_batchWriter": {
						"op_log_0_3_compress",
					},
					"op_log_0_3_compress": {
						"sink_log_0",
					},
				},
			},
		},
		{
			name: "encrypt and compress and cache sink plan",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"compression": "gzip",
							"encryption":  "aes",
							"enableCache": true,
						},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_log_0_0_transform",
					},
					"op_log_0_0_transform": {
						"op_log_0_1_encode",
					},
					"op_log_0_1_encode": {
						"op_log_0_2_compress",
					},
					"op_log_0_2_compress": {
						"op_log_0_3_encrypt",
					},
					"op_log_0_3_encrypt": {
						"op_log_0_4_cache",
					},
					"op_log_0_4_cache": {
						"sink_log_0",
					},
				},
			},
		},
		{
			name: "encrypt and compress with stream writer",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"file": map[string]any{
							"compression": "gzip",
							"encryption":  "aes",
						},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_file_0_0_transform",
					},
					"op_file_0_0_transform": {
						"op_file_0_1_encode",
					},
					"op_file_0_1_encode": {
						"sink_file_0",
					},
				},
			},
		},
		{
			name: "encrypt and compress with tuple collector",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"memory": map[string]any{
							"compression": "gzip",
							"encryption":  "aes",
						},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_memory_0_0_transform",
					},
					"op_memory_0_0_transform": {
						"sink_memory_0",
					},
				},
			},
		},
		{
			name: "resend sink plan",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"enableCache":      true,
							"resendAlterQueue": true,
						},
					},
				},
				Options: defaultOption,
			},
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_log_0_0_transform",
					},
					"op_log_0_0_transform": {
						"op_log_0_1_encode",
					},
					"op_log_0_1_encode": {
						"sink_log_0",
					},
					"sink_log_0": {
						"op_log_0_cache",
					},
					"op_log_0_cache": {
						"sink_log_0_resend",
					},
				},
			},
		},
	}
	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			tp, err := topo.NewWithNameAndOptions("test", c.rule.Options)
			assert.NoError(t, err)
			si, err := io.Source("memory")
			assert.NoError(t, err)
			n, err := node.NewSourceNode(tp.GetContext(), "src1", si, map[string]any{"datasource": "demo"}, &def.RuleOption{SendError: false})
			assert.NoError(t, err)
			tp.AddSrc(n)
			inputs := []node.Emitter{n}
			err = buildActions(tp, c.rule, inputs, 1)
			assert.NoError(t, err)
			assert.Equal(t, c.topo, tp.GetTopo())
		})
	}
}

func TestSinkPlanError(t *testing.T) {
	tc := []struct {
		name string
		rule *def.Rule
		err  string
	}{
		{
			name: "invalid sink",
			rule: &def.Rule{
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
			rule: &def.Rule{
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
			rule: &def.Rule{
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
			rule: &def.Rule{
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
			err: "fail to parse sink configuration: invalid lingerInterval -1000000, must be positive",
		},
		{
			name: "invalid dataTemplate",
			rule: &def.Rule{
				Actions: []map[string]any{
					{
						"log": map[string]any{
							"dataTemplate": "{{...a}}",
						},
					},
				},
				Options: defaultOption,
			},
			err: "template: sink:1: unexpected <.> in operand",
		},
	}
	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			tp, err := topo.NewWithNameAndOptions("test", c.rule.Options)
			assert.NoError(t, err)
			si, err := io.Source("memory")
			assert.NoError(t, err)
			n, err := node.NewSourceNode(tp.GetContext(), "src1", si, map[string]any{"datasource": "demo"}, &def.RuleOption{SendError: false})
			assert.NoError(t, err)
			tp.AddSrc(n)
			inputs := []node.Emitter{n}
			err = buildActions(tp, c.rule, inputs, 1)
			assert.Error(t, err)
			assert.Equal(t, c.err, err.Error())
		})
	}
}

func TestFindTemplates(t *testing.T) {
	cases := []struct {
		name   string
		props  map[string]any
		result []string
	}{
		{
			name: "normal",
			props: map[string]any{
				"test":  1,
				"topic": "{{.topic}}",
				"tt":    50,
				"path":  "mypath/{{.path}}",
			},
			result: []string{
				"mypath/{{.path}}", "{{.topic}}",
			},
		},
		{
			name: "embed",
			props: map[string]any{
				"test":  1,
				"topic": "{{.topic}}",
				"multi": map[string]any{
					"test":  1,
					"topic": "{{.ntopic}}",
				},
				"tt":   50,
				"path": "mypath/{{.path}}",
			},
			result: []string{
				"mypath/{{.path}}", "{{.ntopic}}", "{{.topic}}",
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := findTemplateProps(tt.props)
			sort.Strings(r)
			assert.Equal(t, tt.result, r)
		})
	}
}
