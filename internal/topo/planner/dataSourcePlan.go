// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type DataSourcePlan struct {
	baseLogicalPlan
	name ast.StreamName
	// calculated properties
	// initialized with stream definition, pruned with rule
	metaFields []string
	// pass-on and converted state. For schemaless, the value is always nil
	streamFields map[string]*ast.JsonStreamField
	// pass-on properties
	isSchemaless    bool
	streamStmt      *ast.StreamStmt
	allMeta         bool
	isBinary        bool
	iet             bool
	timestampFormat string
	timestampField  string
	// col -> alias
	colAliasMapping map[string]string
	// intermediate status
	isWildCard  bool
	fields      map[string]*ast.JsonStreamField
	metaMap     map[string]string
	pruneFields []string
	// inRuleTest means whether in the rule test mode
	inRuleTest    bool
	useSliceTuple bool
}

func (p DataSourcePlan) Init() *DataSourcePlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(DATASOURCE)
	return &p
}

func (p *DataSourcePlan) BuildSchemaInfo(ruleID string) {
	schemaInfo := p.buildSchemaInfo(ruleID)
	if schemaInfo != "" {
		p.ExplainInfo.Info += schemaInfo
	}
}

func (p *DataSourcePlan) buildSchemaInfo(ruleID string) string {
	r := schema.GetRuleSchema(ruleID)
	if r.Wildcard != nil && r.Wildcard[string(p.name)] {
		return " wildcard:true"
	}
	if r.Schema != nil && len(r.Schema[string(p.name)]) > 0 {
		b := bytes.NewBufferString(" ConverterSchema:[")
		i := 0
		for colName := range r.Schema[string(p.name)] {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(colName)
			i++
		}
		b.WriteString("]")
		return b.String()
	}
	return ""
}

func (p *DataSourcePlan) BuildExplainInfo() {
	info := ""
	if p.name != "" {
		info += "StreamName: " + string(p.name)
	}
	if len(p.fields) != 0 {
		info += ", Fields:[ "
		keys := make([]string, 0, len(p.fields))
		for k := range p.fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i := 0; i < len(keys); i++ {
			info += keys[i]
			if i != len(keys)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	if len(p.streamFields) != 0 {
		info += ", StreamFields:[ "
		keys := make([]string, 0, len(p.streamFields))
		for k := range p.streamFields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i := 0; i < len(keys); i++ {
			info += keys[i]
			if i != len(keys)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	if len(p.colAliasMapping) > 0 {
		info += ", ColAliasMapping:[ "
		keys := make([]string, 0)
		for col, alias := range p.colAliasMapping {
			keys = append(keys, col+":"+alias)
		}
		sort.Strings(keys)
		for i := 0; i < len(keys); i++ {
			info += keys[i]
			if i != len(keys)-1 {
				info += ", "
			}
		}
		info += " ]"
	}
	p.baseLogicalPlan.ExplainInfo.Info = info
}

// PushDownPredicate Presume no children for data source
func (p *DataSourcePlan) PushDownPredicate(condition ast.Expr) (ast.Expr, LogicalPlan) {
	if p.streamStmt.StreamType == ast.TypeTable {
		return condition, p.self
	}
	owned, other := p.extract(condition)
	if owned != nil {
		// Add a filter plan for children
		f := FilterPlan{
			condition: owned,
		}.Init()
		f.SetChildren([]LogicalPlan{p})
		return other, f
	}
	return other, p
}

func (p *DataSourcePlan) extract(expr ast.Expr) (ast.Expr, ast.Expr) {
	s, hasDefault := getRefSources(expr)
	l := len(s)
	if hasDefault {
		l += 1
	}
	switch len(s) {
	case 0:
		return expr, nil
	case 1:
		if s[0] == p.name || s[0] == ast.DefaultStream {
			return expr, nil
		} else {
			return nil, expr
		}
	default:
		if be, ok := expr.(*ast.BinaryExpr); ok && be.OP == ast.AND {
			ul, pl := p.extract(be.LHS)
			ur, pr := p.extract(be.RHS)
			owned := combine(ul, ur)
			other := combine(pl, pr)
			return owned, other
		}
		return nil, expr
	}
}

func (p *DataSourcePlan) PruneColumns(fields []ast.Expr) error {
	// init values
	err := p.getProps()
	if err != nil {
		return err
	}
	p.fields = make(map[string]*ast.JsonStreamField)
	p.pruneFields = make([]string, 0)
	if !p.allMeta {
		p.metaMap = make(map[string]string)
	}
	arrowFileds := make([]*ast.BinaryExpr, 0)
	for _, field := range fields {
		switch f := field.(type) {
		case *ast.BinaryExpr:
			if f.OP == ast.ARROW {
				// only allowed case like a.b.c
				valid := true
				ast.WalkFunc(f, func(node ast.Node) bool {
					switch c := node.(type) {
					case *ast.BinaryExpr:
						if c.OP != ast.ARROW {
							valid = false
							return false
						}
					case *ast.FieldRef:
						if !c.IsColumn() {
							valid = false
							return false
						}
					case *ast.JsonFieldRef:
					case *ast.MetaRef:
						valid = false
						if p.allMeta {
							break
						}
						if c.StreamName == ast.DefaultStream || c.StreamName == p.name {
							if c.Name == "*" {
								p.allMeta = true
								p.metaMap = nil
							} else if !p.allMeta {
								p.metaMap[strings.ToLower(c.Name)] = c.Name
							}
						}
						return false
					default:
						valid = false
						return false
					}
					return true
				})
				if valid {
					arrowFileds = append(arrowFileds, f)
				}
			}
		case *ast.Wildcard:
			p.isWildCard = true
			p.pruneFields = append(p.pruneFields, f.Except...)
			for _, replace := range f.Replace {
				p.pruneFields = append(p.pruneFields, replace.AName)
			}
		case *ast.FieldRef:
			if !p.isWildCard && (f.StreamName == ast.DefaultStream || f.StreamName == p.name) {
				if _, ok := p.fields[f.Name]; !ok {
					sf, err := p.getField(f.Name, f.StreamName == p.name)
					if err != nil {
						return err
					}
					if p.isSchemaless || sf != nil {
						p.fields[f.Name] = sf
					}
				}
			}
		case *ast.MetaRef:
			if p.allMeta {
				break
			}
			if f.StreamName == ast.DefaultStream || f.StreamName == p.name {
				if f.Name == "*" {
					p.allMeta = true
					p.metaMap = nil
				} else if !p.allMeta {
					p.metaMap[strings.ToLower(f.Name)] = f.Name
				}
			}
		case *ast.SortField:
			if !p.isWildCard {
				sf, err := p.getField(f.Name, f.StreamName == p.name)
				if err != nil {
					return err
				}
				if p.isSchemaless || sf != nil {
					p.fields[f.Name] = sf
				}
			}
		default:
			return fmt.Errorf("unsupported field %v", field)
		}
	}
	if p.timestampField != "" {
		if !p.isSchemaless {
			tsf, ok := p.streamFields[p.timestampField]
			if !ok {
				return fmt.Errorf("timestamp field %s not found", p.timestampField)
			}
			p.fields[p.timestampField] = tsf
		} else {
			p.fields[p.timestampField] = nil
		}
	}
	p.getAllFields()
	if !p.isSchemaless {
		p.handleArrowFields(arrowFileds)
	}
	return nil
}

func buildArrowReference(cur ast.Expr, root map[string]interface{}) (map[string]interface{}, string) {
	switch c := cur.(type) {
	case *ast.BinaryExpr:
		node, name := buildArrowReference(c.LHS, root)
		m := node[name].(map[string]interface{})
		subName := c.RHS.(*ast.JsonFieldRef).Name
		_, ok := m[subName]
		if !ok {
			m[subName] = map[string]interface{}{}
		}
		return m, subName
	case *ast.FieldRef:
		_, ok := root[c.Name]
		if !ok {
			root[c.Name] = map[string]interface{}{}
		}
		return root, c.Name
	}
	return nil, ""
}

// handleArrowFields mark the field and subField for the arrowFields which should be remained
// Then pruned the field which is not used.
func (p *DataSourcePlan) handleArrowFields(arrowFields []*ast.BinaryExpr) {
	root := make(map[string]interface{})
	for _, af := range arrowFields {
		buildArrowReference(af, root)
	}
	for filedName, node := range root {
		jsonStreamField, err := p.getField(filedName, true)
		if err != nil {
			continue
		}
		markPruneJSONStreamField(node, jsonStreamField)
	}
	for key, field := range p.streamFields {
		if field != nil && field.Type == "struct" {
			if !field.Selected {
				delete(p.streamFields, key)
				continue
			}
			pruneJSONStreamField(field)
		}
	}
}

func pruneJSONStreamField(cur *ast.JsonStreamField) {
	cur.Selected = false
	if cur.Type != "struct" {
		return
	}
	for key, subField := range cur.Properties {
		if !subField.Selected {
			delete(cur.Properties, key)
		}
		pruneJSONStreamField(subField)
	}
}

func markPruneJSONStreamField(cur interface{}, field *ast.JsonStreamField) {
	field.Selected = true
	if field.Type != "struct" {
		return
	}
	curM, ok := cur.(map[string]interface{})
	if !ok || len(curM) < 1 {
		return
	}
	for filedName, v := range curM {
		if subField, ok := field.Properties[filedName]; ok {
			markPruneJSONStreamField(v, subField)
		}
	}
}

func (p *DataSourcePlan) getField(name string, strict bool) (*ast.JsonStreamField, error) {
	for col, alias := range p.colAliasMapping {
		if name == alias {
			name = col
			break
		}
	}
	if !p.isSchemaless {
		r, ok := p.streamFields[name]
		if !ok {
			if strict {
				return nil, fmt.Errorf("field %s not found in stream %s", name, p.name)
			}
		} else {
			return r, nil
		}
	}
	// always return nil for schemaless
	return nil, nil
}

// Do not prune fields now for preprocessor
// TODO provide field information to the source for it to prune
func (p *DataSourcePlan) getAllFields() {
	if !p.isWildCard {
		p.streamFields = p.fields
	} else {
		if len(p.fields) > 0 && p.streamFields == nil {
			p.streamFields = make(map[string]*ast.JsonStreamField, len(p.fields))
		}
		for name, fr := range p.fields {
			p.streamFields[name] = fr
		}
	}
	for _, pf := range p.pruneFields {
		prune := true
		for f := range p.fields {
			if pf == f {
				prune = false
				break
			}
		}
		if prune {
			delete(p.streamFields, pf)
		}
	}

	p.metaFields = make([]string, 0, len(p.metaMap))
	for _, v := range p.metaMap {
		p.metaFields = append(p.metaFields, v)
	}
	// for consistency of results for testing
	sort.Strings(p.metaFields)
	p.fields = nil
	p.metaMap = nil
	index := 0
	if conf.IsTesting {
		keys := make([]string, 0, len(p.streamFields))
		for k := range p.streamFields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := p.streamFields[k]
			if p.useSliceTuple {
				if v != nil {
					v.HasIndex = true
					v.Index = index
				} else {
					v = &ast.JsonStreamField{
						Index:    index,
						HasIndex: true,
					}
				}
			}
			p.streamFields[k] = v
			index++
		}
	} else {
		for k, v := range p.streamFields {
			if p.useSliceTuple {
				if v != nil {
					v.HasIndex = true
					v.Index = index
				} else {
					v = &ast.JsonStreamField{
						Index:    index,
						HasIndex: true,
					}
				}
			}
			p.streamFields[k] = v
			index++
		}
	}
}

func (p *DataSourcePlan) getProps() error {
	if p.iet {
		if p.streamStmt.Options.TIMESTAMP != "" {
			p.timestampField = p.streamStmt.Options.TIMESTAMP
		} else {
			return fmt.Errorf("preprocessor is set to be event time but stream option TIMESTAMP not found")
		}
	}
	if p.streamStmt.Options.TIMESTAMP_FORMAT != "" {
		p.timestampFormat = p.streamStmt.Options.TIMESTAMP_FORMAT
	}
	if strings.EqualFold(p.streamStmt.Options.FORMAT, message.FormatBinary) {
		p.isBinary = true
	}
	return nil
}
