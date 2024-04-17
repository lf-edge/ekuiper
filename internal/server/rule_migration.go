// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package server

import (
	"encoding/json"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	store2 "github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type RuleMigrationProcessor struct {
	r *processor.RuleProcessor
	s *processor.StreamProcessor
}

type InstallScriptGetter interface {
	InstallScript(s string) (string, string)
}

func NewRuleMigrationProcessor(r *processor.RuleProcessor, s *processor.StreamProcessor) *RuleMigrationProcessor {
	return &RuleMigrationProcessor{
		r: r,
		s: s,
	}
}

func newDependencies() *dependencies {
	return &dependencies{
		sourceConfigKeys: map[string][]string{},
		sinkConfigKeys:   map[string][]string{},
	}
}

// dependencies copy all connections related configs by hardcode
type dependencies struct {
	rules            []string
	streams          []string
	tables           []string
	sources          []string
	sinks            []string
	sourceConfigKeys map[string][]string
	sinkConfigKeys   map[string][]string
	functions        []string
	schemas          []string
}

func ruleTraverse(rule *def.Rule, de *dependencies) {
	sql := rule.Sql
	if sql != "" {
		stmt, err := xsql.GetStatementFromSql(sql)
		if err != nil {
			return
		}
		store, err := store2.GetKV("stream")
		if err != nil {
			return
		}
		// streams
		streamsFromStmt := xsql.GetStreams(stmt)
		for _, s := range streamsFromStmt {
			streamStmt, err := xsql.GetDataSource(store, s)
			if err != nil {
				continue
			}
			if streamStmt.StreamType == ast.TypeStream {
				// get streams
				de.streams = append(de.streams, string(streamStmt.Name))
			} else if streamStmt.StreamType == ast.TypeTable {
				// get tables
				de.tables = append(de.tables, string(streamStmt.Name))
			}

			// get source type
			de.sources = append(de.sources, streamStmt.Options.TYPE)
			// get config key
			_, ok := de.sourceConfigKeys[streamStmt.Options.TYPE]
			if ok {
				de.sourceConfigKeys[streamStmt.Options.TYPE] = append(de.sourceConfigKeys[streamStmt.Options.TYPE], streamStmt.Options.CONF_KEY)
			} else {
				var confKeys []string
				confKeys = append(confKeys, streamStmt.Options.CONF_KEY)
				de.sourceConfigKeys[streamStmt.Options.TYPE] = confKeys
			}

			// get schema id
			if streamStmt.Options.SCHEMAID != "" {
				r := strings.Split(streamStmt.Options.SCHEMAID, ".")
				de.schemas = append(de.schemas, streamStmt.Options.FORMAT+"_"+r[0])
			}
		}
		// actions
		for _, m := range rule.Actions {
			for name, action := range m {
				props, _ := action.(map[string]interface{})
				de.sinks = append(de.sinks, name)
				resourceId, ok := props[conf.ResourceID].(string)
				if ok {
					_, ok := de.sinkConfigKeys[name]
					if ok {
						de.sinkConfigKeys[name] = append(de.sinkConfigKeys[name], resourceId)
					} else {
						var confKeys []string
						confKeys = append(confKeys, resourceId)
						de.sinkConfigKeys[name] = confKeys
					}
				}

				format, ok := props["format"].(string)
				if ok && format != "json" {
					schemaId, ok := props["schemaId"].(string)
					if ok {
						r := strings.Split(schemaId, ".")
						de.schemas = append(de.schemas, format+"_"+r[0])
					}
				}
			}
		}
		// function
		ast.WalkFunc(stmt, func(n ast.Node) bool {
			switch f := n.(type) {
			case *ast.Call:
				de.functions = append(de.functions, f.Name)
			}
			return true
		})

		// Rules
		de.rules = append(de.rules, rule.Id)
	}
}

func (p *RuleMigrationProcessor) ConfigurationPartialExport(rules []string) ([]byte, error) {
	config := &Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          map[string]string{},
	}
	config.Rules = p.exportRules(rules)

	de := newDependencies()
	for _, v := range rules {
		rule, _ := p.r.GetRuleById(v)
		if rule != nil {
			ruleTraverse(rule, de)
		}
	}

	p.exportSelected(de, config)

	return json.Marshal(config)
}

func (p *RuleMigrationProcessor) exportRules(rules []string) map[string]string {
	ruleSet := make(map[string]string)

	for _, v := range rules {
		ruleJson, _ := p.r.GetRuleJson(v)
		ruleSet[v] = ruleJson
	}
	return ruleSet
}

func (p *RuleMigrationProcessor) exportStreams(streams []string) map[string]string {
	streamSet := make(map[string]string)

	for _, v := range streams {
		streamJson, _ := p.s.GetStream(v, ast.TypeStream)
		streamSet[v] = streamJson
	}
	return streamSet
}

func (p *RuleMigrationProcessor) exportTables(tables []string) map[string]string {
	tableSet := make(map[string]string)

	for _, v := range tables {
		tableJson, _ := p.s.GetStream(v, ast.TypeTable)
		tableSet[v] = tableJson
	}
	return tableSet
}

func (p *RuleMigrationProcessor) exportSelected(de *dependencies, config *Configuration) {
	// get the stream and table
	config.Streams = p.exportStreams(de.streams)
	config.Tables = p.exportTables(de.tables)
	//// get the sources
	//for _, v := range de.sources {
	//	t, srcName, srcInfo := io.GetSourcePlugin(v)
	//	if t == plugin.NATIVE_EXTENSION {
	//		config.NativePlugins[srcName] = srcInfo
	//	}
	//	if t == plugin.PORTABLE_EXTENSION {
	//		config.PortablePlugins[srcName] = srcInfo
	//	}
	//}
	//// get sinks
	//for _, v := range de.sinks {
	//	t, sinkName, sinkInfo := io.GetSinkPlugin(v)
	//	if t == plugin.NATIVE_EXTENSION {
	//		config.NativePlugins[sinkName] = sinkInfo
	//	}
	//	if t == plugin.PORTABLE_EXTENSION {
	//		config.PortablePlugins[sinkName] = sinkInfo
	//	}
	//}
	//
	//// get functions
	//for _, v := range de.functions {
	//	t, svcName, svcInfo := function.GetFunctionPlugin(v)
	//	if t == plugin.NATIVE_EXTENSION {
	//		config.NativePlugins[svcName] = svcInfo
	//	}
	//	if t == plugin.PORTABLE_EXTENSION {
	//		config.PortablePlugins[svcName] = svcInfo
	//	}
	//	if t == plugin.SERVICE_EXTENSION {
	//		config.Service[svcName] = svcInfo
	//	}
	//}

	// get sourceCfg/sinkCfg
	configKeys := meta.YamlConfigurationKeys{}
	configKeys.Sources = de.sourceConfigKeys
	configKeys.Sinks = de.sinkConfigKeys
	configSet := meta.GetConfigurationsFor(configKeys)
	config.SourceConfig = configSet.Sources
	config.SinkConfig = configSet.Sinks
	config.ConnectionConfig = configSet.Connections

	// get schema
	if managers["schema"] != nil {
		f, ok := managers["schema"].(InstallScriptGetter)
		if ok {
			for _, v := range de.schemas {
				schName, schInfo := f.InstallScript(v)
				config.Schema[schName] = schInfo
			}
		} else { // should never happen
			logger.Errorf("schema manager is not InstallScriptGetter")
		}
	}

	config.Uploads = uploadsExport()
}
