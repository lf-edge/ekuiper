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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestPlanTopo(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	schema.InitRegistry()
	modules.RegisterConverter("mockp", func(ctx api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &message.MockPartialConverter{}, nil
	})
	modules.RegisterMerger("mock", func(ctx api.StreamContext, schemaId string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		return &message.MockMerger{}, nil
	})
	delete(modules.ConverterSchemas, "mock")
	delete(modules.ConverterSchemas, "mockp")
	streamSqls := map[string]string{
		"src1":     `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt");`,
		"src2":     `CREATE STREAM src2 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt", SHARED="true");`,
		"src3":     `CREATE STREAM src3 () WITH (DATASOURCE="topic1", FORMAT="mockp", TYPE="mqtt", CONF_KEY="testSelMock");`,
		"src4":     `CREATE STREAM src4 () WITH (DATASOURCE="topic1", FORMAT="json", TYPE="mqtt", CONF_KEY="testSel",SHARED="true");`,
		"src5":     `CREATE STREAM src3 () WITH (DATASOURCE="topic1", FORMAT="json", TYPE="mqtt", CONF_KEY="testSelMerger");`,
		"filesrc1": `CREATE STREAM fs1 () WITH (FORMAT="json", TYPE="file",CONF_KEY="lines");`,
		"filesrc2": `CREATE STREAM fs2 () WITH (FORMAT="delimited", TYPE="file",CONF_KEY="csv");`,
		"filesrc3": `CREATE STREAM fs3 () WITH (FORMAT="json",TYPE="file",CONF_KEY="json");`,
		"neuron1":  `CREATE STREAM neuron1 () WITH (FORMAT="json", TYPE="neuron",CONF_KEY="tcp");`,
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
				"connectionSelector": "mqtt.localConnection",
				"interval":           "1s",
				"mergeField":         "id",
			},
			p: "mqtt",
			k: "testSelMock",
		},
		{
			conf: map[string]any{
				"connectionSelector": "mqtt.localConnection",
				"interval":           "1s",
				"merger":             "mock",
			},
			p: "mqtt",
			k: "testSelMerger",
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
		{
			conf: map[string]any{
				"url": "tcp://127.0.0.1:7777",
			},
			p: "neuron",
			k: "tcp",
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
						"op_2_emitter",
					},
					"op_2_emitter": {
						"op_3_ratelimit",
					},
					"op_3_ratelimit": {
						"op_4_payload_decoder",
					},
					"op_4_payload_decoder": {
						"op_5_project",
					},
					"op_5_project": {
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
						"op_src4_2_emitter",
					},
					"op_src4_2_emitter": {
						"op_src4_3_ratelimit",
					},
					"op_src4_3_ratelimit": {
						"op_src4_4_decoder",
					},
					"op_src4_4_decoder": {
						"op_5_project",
					},
					"op_5_project": {
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
		{
			name: "test mqtt merger",
			sql:  `SELECT * FROM src5`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_mqtt.localConnection/topic1"},
				Edges: map[string][]any{
					"source_mqtt.localConnection/topic1": {
						"op_2_emitter",
					},
					"op_2_emitter": {
						"op_3_ratelimit",
					},
					"op_3_ratelimit": {
						"op_4_payload_decoder",
					},
					"op_4_payload_decoder": {
						"op_5_project",
					},
					"op_5_project": {
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
			name: "testNngConnSplit",
			sql:  `SELECT * FROM neuron1`,
			topo: &def.PrintableTopo{
				Sources: []string{"source_nng:pairtcp://127.0.0.1:7777/singleton"},
				Edges: map[string][]any{
					"source_nng:pairtcp://127.0.0.1:7777/singleton": {
						"op_2_emitter",
					},
					"op_2_emitter": {
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
			tp, _, err := PlanSQLWithSourcesAndSinks(def.GetDefaultRule(tt.name, tt.sql), nil)
			assert.NoError(t, err)
			assert.Equal(t, tt.topo, tp.GetTopo())
		})
	}
	r := def.GetDefaultRule("incplan", "select count(*) from src1 group by countwindow(2) filter (where a > 1)")
	r.Options.PlanOptimizeStrategy.EnableIncrementalWindow = true
	_, _, err = PlanSQLWithSourcesAndSinks(r, nil)
	assert.NoError(t, err)
}

func TestSourceErr(t *testing.T) {
	conf.IsTesting = true
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, conf.WriteCfgIntoKVStorage("sources", "mqtt", "srcTest1", map[string]interface{}{"server": 123}))
	streamSqls := map[string]string{
		"srcErr1": `CREATE STREAM srcErr1 () WITH (CONF_KEY="srcErr1", FORMAT="json", TYPE="mqtt",DATASOURCE="t1");`,
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
	_, _, err = PlanSQLWithSourcesAndSinks(def.GetDefaultRule("srcErr1", "select * from srcErr1"), nil)
	assert.NoError(t, err)
	require.NoError(t, conf.WriteCfgIntoKVStorage("sources", "mqtt", "srcTest1", map[string]interface{}{"server": "tcp://127.0.0.1:1883"}))
	_, _, err = PlanSQLWithSourcesAndSinks(def.GetDefaultRule("srcErr1", "select * from srcErr1"), nil)
	assert.NoError(t, err)
}

func TestPlanError(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	modules.RegisterConverter("mockp", func(ctx api.StreamContext, schemaFileName string, logicalSchema map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &message.MockPartialConverter{}, nil
	})
	modules.RegisterMerger("mock", func(ctx api.StreamContext, schemaId string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		return &message.MockMerger{}, nil
	})
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt", CONF_KEY="invalidMerge");`,
		"src2": `CREATE STREAM src2 () WITH (DATASOURCE="src1", FORMAT="", TYPE="mqtt", CONF_KEY="invalidMerger");`,
		"src3": `CREATE STREAM src2 () WITH (DATASOURCE="src1", FORMAT="json", TYPE="mqtt", CONF_KEY="invalidMerger2");`,
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
				"interval":   "1s",
				"mergeField": "id",
				"merger":     "mock",
			},
			p: "mqtt",
			k: "invalidMerge",
		},
		{
			conf: map[string]any{
				"interval": "1s",
				"merger":   "mock",
			},
			p: "mqtt",
			k: "invalidMerger",
		},
		{
			conf: map[string]any{
				"merger":        "mock",
				"payloadFormat": "mockp",
			},
			p: "mqtt",
			k: "invalidMerger2",
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
		e    string
	}{
		{
			name: "mergeField and merger mutual exclusive",
			sql:  `SELECT * FROM src1`,
			e:    "mergeField and merger cannot set together",
		},
		{
			name: "merger need rate limit",
			sql:  `SELECT * FROM src3`,
			e:    "merger is set but rate limit is not required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := PlanSQLWithSourcesAndSinks(def.GetDefaultRule(tt.name, tt.sql), nil)
			assert.Error(t, err)
			assert.EqualError(t, err, tt.e)
		})
	}
}

func TestPlanLookup(t *testing.T) {
	ctx := mockContext.NewMockContext("Test", "test")
	t.Run("undefined source", func(t *testing.T) {
		_, err := planLookupSource(ctx, &LookupPlan{options: &ast.Options{TYPE: "none"}}, &def.RuleOption{})
		assert.Error(t, err)
		assert.EqualError(t, err, "lookup source type none not found")
	})
	t.Run("missing format", func(t *testing.T) {
		modules.RegisterLookupSource("mock", func() api.Source {
			return &MockLookupBytes{}
		})
		_, err := planLookupSource(ctx, &LookupPlan{options: &ast.Options{TYPE: "mock"}}, &def.RuleOption{})
		assert.Error(t, err)
		assert.EqualError(t, err, "lookup source type mock must specify format")
	})
	t.Run("register wrong source", func(t *testing.T) {
		modules.RegisterLookupSource("mock", mqtt.GetSource)
		_, err := planLookupSource(ctx, &LookupPlan{options: &ast.Options{TYPE: "mock"}}, &def.RuleOption{})
		assert.Error(t, err)
		assert.EqualError(t, err, "got non lookup source mock")
	})
}

type MockLookupBytes struct{}

func (m *MockLookupBytes) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *MockLookupBytes) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockLookupBytes) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *MockLookupBytes) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([][]byte, error) {
	return nil, nil
}
