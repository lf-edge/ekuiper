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

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/meta"
	store2 "github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/topo/graph"
	"github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

type RuleMigrationProcessor struct {
	r *processor.RuleProcessor
	s *processor.StreamProcessor
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

func ruleTraverse(rule *api.Rule, de *dependencies) {
	sql := rule.Sql
	ruleGraph := rule.Graph
	if sql != "" {
		stmt, err := xsql.GetStatementFromSql(sql)
		if err != nil {
			return
		}
		store, err := store2.GetKV("stream")
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
				de.streams = append(de.streams, string(streamStmt.Name))
			} else if streamStmt.StreamType == ast.TypeTable {
				//get tables
				de.tables = append(de.tables, string(streamStmt.Name))
			}

			//get source type
			de.sources = append(de.sources, streamStmt.Options.TYPE)
			//get config key
			_, ok := de.sourceConfigKeys[streamStmt.Options.TYPE]
			if ok {
				de.sourceConfigKeys[streamStmt.Options.TYPE] = append(de.sourceConfigKeys[streamStmt.Options.TYPE], streamStmt.Options.CONF_KEY)
			} else {
				var confKeys []string
				confKeys = append(confKeys, streamStmt.Options.CONF_KEY)
				de.sourceConfigKeys[streamStmt.Options.TYPE] = confKeys
			}

			//get schema id
			if streamStmt.Options.SCHEMAID != "" {
				r := strings.Split(streamStmt.Options.SCHEMAID, ".")
				de.schemas = append(de.schemas, streamStmt.Options.FORMAT+"_"+r[0])
			}
		}
		//actions
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

		//Rules
		de.rules = append(de.rules, rule.Id)
	} else {

		for _, gn := range ruleGraph.Nodes {
			switch gn.Type {
			case "source":
				sourceOption := &ast.Options{}
				err := cast.MapToStruct(gn.Props, sourceOption)
				if err != nil {
					break
				}
				sourceOption.TYPE = gn.NodeType

				de.sources = append(de.sources, sourceOption.TYPE)
				//get config key
				_, ok := de.sourceConfigKeys[sourceOption.TYPE]
				if ok {
					de.sourceConfigKeys[sourceOption.TYPE] = append(de.sourceConfigKeys[sourceOption.TYPE], sourceOption.CONF_KEY)
				} else {
					var confKeys []string
					confKeys = append(confKeys, sourceOption.CONF_KEY)
					de.sourceConfigKeys[sourceOption.TYPE] = confKeys
				}
				//get schema id
				if sourceOption.SCHEMAID != "" {
					r := strings.Split(sourceOption.SCHEMAID, ".")
					de.schemas = append(de.schemas, sourceOption.FORMAT+"_"+r[0])
				}
			case "sink":
				sinkType := gn.NodeType
				props := gn.Props
				de.sinks = append(de.sinks, sinkType)
				resourceId, ok := props[conf.ResourceID].(string)
				if ok {
					_, ok := de.sinkConfigKeys[sinkType]
					if ok {
						de.sinkConfigKeys[sinkType] = append(de.sinkConfigKeys[sinkType], resourceId)
					} else {
						var confKeys []string
						confKeys = append(confKeys, resourceId)
						de.sinkConfigKeys[sinkType] = confKeys
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
			case "operator":
				nt := strings.ToLower(gn.NodeType)
				switch nt {
				case "function":
					fop, err := parseFunc(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(fop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "aggfunc":
					fop, err := parseFunc(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(fop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "filter":
					fop, err := parseFilter(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(fop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "pick":
					pop, err := parsePick(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(pop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "join":
					jop, err := parseJoin(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(jop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "groupby":
					gop, err := parseGroupBy(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(gop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "orderby":
					oop, err := parseOrderBy(gn.Props)
					if err != nil {
						break
					}
					ast.WalkFunc(oop, func(n ast.Node) bool {
						switch f := n.(type) {
						case *ast.Call:
							de.functions = append(de.functions, f.Name)
						}
						return true
					})
				case "switch":
					opArray, err := parseSwitch(gn.Props)
					if err != nil {
						break
					}
					for _, op := range opArray {
						ast.WalkFunc(op, func(n ast.Node) bool {
							switch f := n.(type) {
							case *ast.Call:
								de.functions = append(de.functions, f.Name)
							}
							return true
						})
					}
				}
			default:
				break
			}
		}
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
	//get the stream and table
	config.Streams = p.exportStreams(de.streams)
	config.Tables = p.exportTables(de.tables)
	//get the sources
	for _, v := range de.sources {
		t, srcName, srcInfo := io.GetSourcePlugin(v)
		if t == plugin.NATIVE_EXTENSION {
			config.NativePlugins[srcName] = srcInfo
		}
		if t == plugin.PORTABLE_EXTENSION {
			config.PortablePlugins[srcName] = srcInfo
		}
	}
	// get sinks
	for _, v := range de.sinks {
		t, sinkName, sinkInfo := io.GetSinkPlugin(v)
		if t == plugin.NATIVE_EXTENSION {
			config.NativePlugins[sinkName] = sinkInfo
		}
		if t == plugin.PORTABLE_EXTENSION {
			config.PortablePlugins[sinkName] = sinkInfo
		}
	}

	// get functions
	for _, v := range de.functions {
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
	configKeys.Sources = de.sourceConfigKeys
	configKeys.Sinks = de.sinkConfigKeys
	configSet := meta.GetConfigurationsFor(configKeys)
	config.SourceConfig = configSet.Sources
	config.SinkConfig = configSet.Sinks
	config.ConnectionConfig = configSet.Connections

	//get schema
	for _, v := range de.schemas {
		schName, schInfo := getSchemaInstallScript(v)
		config.Schema[schName] = schInfo
	}
}

func parsePick(props map[string]interface{}) (*ast.SelectStatement, error) {
	n := &graph.Select{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt, err := xsql.NewParser(strings.NewReader("select " + strings.Join(n.Fields, ",") + " from nonexist")).Parse()
	if err != nil {
		return nil, err
	} else {
		return stmt, nil
	}
}

func parseFunc(props map[string]interface{}) (*ast.SelectStatement, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	funcExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	stmt, err := xsql.NewParser(strings.NewReader("select " + funcExpr + " from nonexist")).Parse()
	if err != nil {
		return nil, err
	} else {
		return stmt, nil
	}
}

func parseFilter(props map[string]interface{}) (ast.Expr, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	conditionExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	p := xsql.NewParser(strings.NewReader(" where " + conditionExpr))
	if exp, err := p.ParseCondition(); err != nil {
		return nil, err
	} else {
		return exp, nil
	}

}

func parseHaving(props map[string]interface{}) (ast.Expr, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	conditionExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	p := xsql.NewParser(strings.NewReader("where " + conditionExpr))
	if exp, err := p.ParseCondition(); err != nil {
		return nil, err
	} else {
		return exp, nil
	}
}

func parseSwitch(props map[string]interface{}) ([]ast.Expr, error) {
	n := &graph.Switch{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if len(n.Cases) == 0 {
		return nil, fmt.Errorf("switch node must have at least one case")
	}
	caseExprs := make([]ast.Expr, len(n.Cases))
	for i, c := range n.Cases {
		p := xsql.NewParser(strings.NewReader("where " + c))
		if exp, err := p.ParseCondition(); err != nil {
			return nil, fmt.Errorf("parse case %d error: %v", i, err)
		} else {
			if exp != nil {
				caseExprs[i] = exp
			}
		}
	}
	return caseExprs, nil
}

func parseOrderBy(props map[string]interface{}) (*ast.SelectStatement, error) {
	n := &graph.Orderby{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt := "SELECT * FROM unknown ORDER BY"
	for _, s := range n.Sorts {
		stmt += " " + s.Field + " "
		if s.Desc {
			stmt += "DESC"
		}
	}
	p, err := xsql.NewParser(strings.NewReader(stmt)).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid order by statement error: %v", err)
	} else {
		return p, nil
	}
}

func parseGroupBy(props map[string]interface{}) (*ast.SelectStatement, error) {
	n := &graph.Groupby{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if len(n.Dimensions) == 0 {
		return nil, fmt.Errorf("groupby must have at least one dimension")
	}
	stmt := "SELECT * FROM unknown Group By " + strings.Join(n.Dimensions, ",")
	p, err := xsql.NewParser(strings.NewReader(stmt)).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid join statement error: %v", err)
	} else {
		return p, nil
	}
}

func parseJoin(props map[string]interface{}) (*ast.SelectStatement, error) {
	n := &graph.Join{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt := "SELECT * FROM " + n.From
	for _, join := range n.Joins {
		stmt += " " + join.Type + " JOIN ON " + join.On
	}
	p, err := xsql.NewParser(strings.NewReader(stmt)).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid join statement error: %v", err)
	} else {
		return p, nil
	}

}
