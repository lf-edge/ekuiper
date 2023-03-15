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

package processor

import (
	"encoding/json"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/meta"
	store2 "github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

type RuleMigrationProcessor struct {
	r *RuleProcessor
	s *StreamProcessor
}

func NewRuleMigrationProcessor(r *RuleProcessor, s *StreamProcessor) *RuleMigrationProcessor {
	return &RuleMigrationProcessor{
		r: r,
		s: s,
	}
}

func NewDependencies() *Dependencies {
	return &Dependencies{
		SourceConfigKeys: map[string][]string{},
		SinkConfigKeys:   map[string][]string{},
	}
}

// Dependencies copy all connections related configs by hardcode
type Dependencies struct {
	Rules            []string
	Streams          []string
	Tables           []string
	Sources          []string
	Sinks            []string
	SourceConfigKeys map[string][]string
	SinkConfigKeys   map[string][]string
	Functions        []string
	Schemas          []string
}

func ruleTraverse(rule *api.Rule, de *Dependencies) {
	sql := rule.Sql
	if sql != "" {
		stmt, err := xsql.GetStatementFromSql(sql)
		if err != nil {
			return
		}
		err, store := store2.GetKV("stream")
		if err != nil {
			return
		}
		//streams
		streamsFromStmt := xsql.GetStreams(stmt)
		for _, s := range streamsFromStmt {
			streamStmt, err := xsql.GetDataSource(store, s)
			if err != nil {
				continue
			}
			if streamStmt.StreamType == ast.TypeStream {
				//get streams
				de.Streams = append(de.Streams, string(streamStmt.Name))
			} else if streamStmt.StreamType == ast.TypeTable {
				//get tables
				de.Tables = append(de.Tables, string(streamStmt.Name))
			}

			//get source type
			de.Sources = append(de.Sources, streamStmt.Options.TYPE)
			//get config key
			_, ok := de.SourceConfigKeys[streamStmt.Options.TYPE]
			if ok {
				de.SourceConfigKeys[streamStmt.Options.TYPE] = append(de.SourceConfigKeys[streamStmt.Options.TYPE], streamStmt.Options.CONF_KEY)
			} else {
				var confKeys []string
				confKeys = append(confKeys, streamStmt.Options.CONF_KEY)
				de.SourceConfigKeys[streamStmt.Options.TYPE] = confKeys
			}

			//get schema id
			if streamStmt.Options.SCHEMAID != "" {
				r := strings.Split(streamStmt.Options.SCHEMAID, ".")
				de.Schemas = append(de.Schemas, streamStmt.Options.FORMAT+"_"+r[0])
			}
		}
		//actions
		for _, m := range rule.Actions {
			for name, action := range m {
				props, _ := action.(map[string]interface{})
				de.Sinks = append(de.Sinks, name)
				resourceId, ok := props[conf.ResourceID].(string)
				if ok {
					_, ok := de.SinkConfigKeys[name]
					if ok {
						de.SinkConfigKeys[name] = append(de.SinkConfigKeys[name], resourceId)
					} else {
						var confKeys []string
						confKeys = append(confKeys, resourceId)
						de.SinkConfigKeys[name] = confKeys
					}
				}

				format, ok := props["format"].(string)
				if ok && format != "json" {
					schemaId, ok := props["schemaId"].(string)
					if ok {
						r := strings.Split(schemaId, ".")
						de.Schemas = append(de.Schemas, format+"_"+r[0])
					}
				}
			}
		}
		// function
		ast.WalkFunc(stmt, func(n ast.Node) bool {
			switch f := n.(type) {
			case *ast.Call:
				de.Functions = append(de.Functions, f.Name)
			}
			return true
		})

		//Rules
		de.Rules = append(de.Rules, rule.Id)
	}
}

type Configuration struct {
	Streams          map[string]string `json:"streams"`
	Tables           map[string]string `json:"tables"`
	Rules            map[string]string `json:"rules"`
	NativePlugins    map[string]string `json:"nativePlugins"`
	PortablePlugins  map[string]string `json:"portablePlugins"`
	SourceConfig     map[string]string `json:"sourceConfig"`
	SinkConfig       map[string]string `json:"sinkConfig"`
	ConnectionConfig map[string]string `json:"connectionConfig"`
	Service          map[string]string `json:"Service"`
	Schema           map[string]string `json:"Schema"`
}

func (p *RuleMigrationProcessor) ConfigurationPartialExport(rules []string) ([]byte, error) {
	conf := &Configuration{
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
	}
	conf.Rules = p.exportRules(rules)

	de := NewDependencies()
	for _, v := range rules {
		rule, _ := p.r.GetRuleById(v)
		if rule != nil {
			ruleTraverse(rule, de)
		}
	}

	p.exportSelected(de, conf)

	return json.Marshal(conf)
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

func (p *RuleMigrationProcessor) exportSelected(de *Dependencies, config *Configuration) {
	//get the stream and table
	config.Streams = p.exportStreams(de.Streams)
	config.Tables = p.exportTables(de.Tables)
	//get the sources
	for _, v := range de.Sources {
		t, srcName, srcInfo := io.GetSourcePlugin(v)
		if t == plugin.NATIVE_EXTENSION {
			config.NativePlugins[srcName] = srcInfo
		}
		if t == plugin.PORTABLE_EXTENSION {
			config.PortablePlugins[srcName] = srcInfo
		}
	}
	// get sinks
	for _, v := range de.Sinks {
		t, sinkName, sinkInfo := io.GetSinkPlugin(v)
		if t == plugin.NATIVE_EXTENSION {
			config.NativePlugins[sinkName] = sinkInfo
		}
		if t == plugin.PORTABLE_EXTENSION {
			config.PortablePlugins[sinkName] = sinkInfo
		}
	}

	// get functions
	for _, v := range de.Functions {
		t, svcName, svcInfo := function.GetFunctionPlugin(v)
		if t == plugin.NATIVE_EXTENSION {
			config.NativePlugins[svcName] = svcInfo
		}
		if t == plugin.PORTABLE_EXTENSION {
			config.PortablePlugins[svcName] = svcInfo
		}
		if t == plugin.SERVICE_EXTENSION {
			config.Service[svcName] = svcInfo
		}
	}

	// get sourceCfg/sinkCfg
	configKeys := meta.YamlConfigurationKeys{}
	configKeys.Sources = de.SourceConfigKeys
	configKeys.Sinks = de.SinkConfigKeys
	configSet := meta.GetConfigurationsFor(configKeys)
	config.SourceConfig = configSet.Sources
	config.SinkConfig = configSet.Sinks
	config.ConnectionConfig = configSet.Connections

	//get schema
	for _, v := range de.Schemas {
		schName, schInfo := schema.GetSchemaInstallScript(v)
		config.Schema[schName] = schInfo
	}
}
