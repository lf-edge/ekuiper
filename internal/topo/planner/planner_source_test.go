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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestPlanTopo(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1":     `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt");`,
		"src2":     `CREATE STREAM src2 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt", SHARED="true");`,
		"src3":     `CREATE STREAM src3 () WITH (DATASOURCE="topic1", FORMAT="json", TYPE="mqtt", CONF_KEY="testSel");`,
		"src4":     `CREATE STREAM src4 () WITH (DATASOURCE="topic1", FORMAT="json", TYPE="mqtt", CONF_KEY="testSel",SHARED="true");`,
		"filesrc1": `CREATE STREAM fs1 () WITH (FORMAT="json", TYPE="file",CONF_KEY="lines");`,
		"filesrc2": `CREATE STREAM fs2 () WITH (FORMAT="delimited", TYPE="file",CONF_KEY="csv");`,
		"filesrc3": `CREATE STREAM fs3 () WITH (FORMAT="json",TYPE="file",CONF_KEY="json");`,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: ast.TypeStream,
			Statement:  sql,
		})
		assert.NoError(t, err)
		err = kv.Set(name, string(s))
		assert.NoError(t, err)
	}
	confs := []struct {
		conf map[string]any
		p    string
		k    string
	}{
		{
			conf: map[string]any{
				"connectionSelector": "mqtt.localConnection",
				"interval":           "1s",
			},
			p: "mqtt",
			k: "testSel",
		},
		{
			conf: map[string]any{
				"fileType":      "csv",
				"payloadFormat": "json",
				"payloadField":  "col1",
				"interval":      "20s",
			},
			p: "file",
			k: "csv",
		},
		{
			conf: map[string]any{
				"fileType": "lines",
				"interval": "20s",
			},
			p: "file",
			k: "lines",
		},
		{
			conf: map[string]any{
				"decompression": "gzip",
				"interval":      "20s",
			},
			p: "file",
			k: "json",
		},
	}
	meta.InitYamlConfigManager()
	dataDir, _ := conf.GetDataLoc()
	err = os.MkdirAll(filepath.Join(dataDir, "sources"), 0o755)
	assert.NoError(t, err)

	for _, cc := range confs {
		p, k := cc.p, cc.k
		bs, err := json.Marshal(cc.conf)
		assert.NoError(t, err)
		err = meta.AddSourceConfKey(p, k, "", bs)
		assert.NoError(t, err)
		// intended to run at last
		defer func() {
			err = meta.DelSourceConfKey(p, k, "")
			assert.NoError(t, err)
		}()
	}

	tests := []struct {
		name string
		sql  string
		topo *def.PrintableTopo
	}{
		{
			name: "testMqttSplit",
			sql:  `SELECT * FROM src1`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_src1"},
				Edges: map[string][]any{
					"source_src1": {
						"op_2_decoder",
					},
					"op_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
		{
			name: "testSharedMqttSplit",
			sql:  `SELECT * FROM src2`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_src2"},
				Edges: map[string][]any{
					"source_src2": {
						"op_src2_2_decoder",
					},
					"op_src2_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
		{
			name: "testSharedConnSplit",
			sql:  `SELECT * FROM src3`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_mqtt.localConnection/topic1"},
				Edges: map[string][]any{
					"source_mqtt.localConnection/topic1": {
						"op_2_ratelimit",
					},
					"op_2_ratelimit": {
						"op_3_decoder",
					},
					"op_3_decoder": {
						"op_4_project",
					},
					"op_4_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
		{
			name: "testSharedNodeWithSharedConnSplit",
			sql:  `SELECT * FROM src4`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_mqtt.localConnection/topic1"},
				Edges: map[string][]any{
					"source_mqtt.localConnection/topic1": {
						"op_src4_2_ratelimit",
					},
					"op_src4_2_ratelimit": {
						"op_src4_3_decoder",
					},
					"op_src4_3_decoder": {
						"op_4_project",
					},
					"op_4_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
		{
			name: "test lines file",
			sql:  `SELECT * FROM filesrc1`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_fs1"},
				Edges: map[string][]any{
					"source_fs1": {
						"op_2_decoder",
					},
					"op_2_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
		{
			name: "test csv file",
			sql:  `SELECT * FROM filesrc2`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_fs2"},
				Edges: map[string][]any{
					"source_fs2": {
						"op_2_payload_decoder",
					},
					"op_2_payload_decoder": {
						"op_3_project",
					},
					"op_3_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
		{
			name: "test json file",
			sql:  `SELECT * FROM filesrc3`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_fs3"},
				Edges: map[string][]any{
					"source_fs3": {
						"op_2_decompress",
					},
					"op_2_decompress": {
						"op_3_decoder",
					},
					"op_3_decoder": {
						"op_4_project",
					},
					"op_4_project": {
						"op_logToMemory_0_0_transform",
					},
					"op_logToMemory_0_0_transform": {
						"op_logToMemory_0_1_encode",
					},
					"op_logToMemory_0_1_encode": {
						"sink_logToMemory_0",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp, err := PlanSQLWithSourcesAndSinks(def.GetDefaultRule(tt.name, tt.sql), nil)
			assert.NoError(t, err)
			assert.Equal(t, tt.topo, tp.GetTopo())
		})
	}
}
