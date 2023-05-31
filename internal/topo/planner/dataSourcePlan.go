// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"sort"
	"strings"

	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
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
	// intermediate status
	isWildCard bool
	fields     map[string]*ast.JsonStreamField
	metaMap    map[string]string
}

func (p DataSourcePlan) Init() *DataSourcePlan {
	p.baseLogicalPlan.self = &p
	return &p
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
	if !p.allMeta {
		p.metaMap = make(map[string]string)
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
	for _, field := range fields {
		switch f := field.(type) {
		case *ast.Wildcard:
			p.isWildCard = true
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
	p.getAllFields()
	return nil
}

func (p *DataSourcePlan) getField(name string, strict bool) (*ast.JsonStreamField, error) {
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
	}
	p.metaFields = make([]string, 0, len(p.metaMap))
	for _, v := range p.metaMap {
		p.metaFields = append(p.metaFields, v)
	}
	// for consistency of results for testing
	sort.Strings(p.metaFields)
	p.fields = nil
	p.metaMap = nil
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
