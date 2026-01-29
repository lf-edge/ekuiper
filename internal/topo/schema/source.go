// Copyright 2025 EMQ Technologies Co., Ltd.
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

package schema

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type SharedLayer struct {
	syncx.RWMutex
	schema map[string]*ast.JsonStreamField
	// ruleID -> schema
	// Save the schemainfo for each rule only to use when need to attach schema when the rule is starting.
	// Get updated if the rule is updated. Never delete it until the subtopo is deleted.
	reg map[string]schemainfo
	// ruleID -> StreamName
	streamMap map[string]string
	// ruleID -> wildcard
	wildcardMap map[string]struct{}
	// field name -> index; append only, do not detach. Use this to assign source index for fields
	indexMap map[string]int
}

type schemainfo struct {
	datasource string
	schema     map[string]*ast.JsonStreamField
	isWildcard bool
}

func newSharedLayer() *SharedLayer {
	return &SharedLayer{
		reg:         make(map[string]schemainfo),
		streamMap:   make(map[string]string),
		wildcardMap: make(map[string]struct{}),
		indexMap:    make(map[string]int),
	}
}

func (s *SharedLayer) RegSchema(ruleID, dataSource string, schema map[string]*ast.JsonStreamField, isWildCard bool) {
	s.Lock()
	defer s.Unlock()
	s.reg[ruleID] = schemainfo{
		datasource: dataSource,
		schema:     schema,
		isWildcard: isWildCard,
	}
	for k, f := range schema {
		if f != nil {
			if index, ok := s.indexMap[k]; !ok {
				index = len(s.indexMap)
				s.indexMap[k] = index
				f.Index = index
			} else {
				f.Index = index
			}
		}
		schema[k] = f
	}
}

func (s *SharedLayer) updateReg() {
	if len(s.wildcardMap) > 0 {
		for ruleID := range s.reg {
			AddRuleSchema(ruleID, s.streamMap[ruleID], nil, true)
		}
		return
	}
	for ruleID := range s.reg {
		AddRuleSchema(ruleID, s.streamMap[ruleID], s.schema, false)
	}
}

func (s *SharedLayer) Attach(ctx api.StreamContext) error {
	s.Lock()
	defer s.Unlock()
	ruleID := ctx.GetRuleId()
	info, ok := s.reg[ruleID]
	if !ok {
		return fmt.Errorf("rule schema %s is not registered", ruleID)
	}
	s.streamMap[ruleID] = info.datasource
	if info.isWildcard {
		s.wildcardMap[ruleID] = struct{}{}
	} else {
		delete(s.wildcardMap, ruleID)
		mergedSchema, err := s.merge(s.schema, info.schema)
		if err != nil {
			return err
		}
		s.schema = mergedSchema
	}
	s.updateReg()
	return nil
}

func (s *SharedLayer) Detach(ctx api.StreamContext, isClose bool) error {
	var err error
	s.Lock()
	defer s.Unlock()
	ruleID := ctx.GetRuleId()
	_, ok := s.reg[ruleID]
	if ok {
		RemoveRuleSchema(ruleID)
		if isClose {
			delete(s.streamMap, ruleID)
			delete(s.wildcardMap, ruleID)
			delete(s.reg, ruleID)
			newSchema := make(map[string]*ast.JsonStreamField)
			for _, si := range s.reg {
				newSchema, err = s.merge(newSchema, si.schema)
				if err != nil {
					return err
				}
			}
			s.schema = newSchema
			s.updateReg()
		}
	}
	return nil
}

func (s *SharedLayer) GetSchema() map[string]*ast.JsonStreamField {
	s.RLock()
	defer s.RUnlock()
	if len(s.wildcardMap) > 0 {
		return nil
	}
	return s.schema
}

func (s *SharedLayer) GetSchemaIndex() map[string]int {
	s.RLock()
	defer s.RUnlock()
	if len(s.wildcardMap) > 0 {
		return nil
	}
	return s.indexMap
}

func (s *SharedLayer) merge(originSchema, newSchema map[string]*ast.JsonStreamField) (map[string]*ast.JsonStreamField, error) {
	ss, err := mergeSchema(originSchema, newSchema)
	if err != nil {
		return nil, err
	}
	// update index map
	for k, f := range ss {
		if f != nil {
			if index, ok := s.indexMap[k]; ok {
				f.Index = index
				ss[k] = f
			}
		}
	}
	return ss, nil
}

func mergeSchema(originSchema, newSchema map[string]*ast.JsonStreamField) (map[string]*ast.JsonStreamField, error) {
	resultSchema := make(map[string]*ast.JsonStreamField)
	for ruleID, oldSchemaField := range originSchema {
		resultSchema[ruleID] = oldSchemaField
	}
	for ruleID, newSchemaField := range newSchema {
		oldSchemaField, ok := originSchema[ruleID]
		if ok {
			switch {
			case oldSchemaField != nil && newSchemaField != nil:
				if oldSchemaField.Type != newSchemaField.Type {
					return nil, fmt.Errorf("column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Type, newSchemaField.Type)
				}
				switch oldSchemaField.Type {
				case "struct":
					subResultSchema, err := mergeSchema(oldSchemaField.Properties, newSchemaField.Properties)
					if err != nil {
						return nil, err
					}
					resultSchema[ruleID].Properties = subResultSchema
				case "array":
					if oldSchemaField.Items.Type != newSchemaField.Items.Type {
						return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, newSchemaField.Items.Type)
					}
					if oldSchemaField.Items.Type == "struct" {
						subResultSchema, err := mergeSchema(oldSchemaField.Items.Properties, newSchemaField.Items.Properties)
						if err != nil {
							return nil, err
						}
						resultSchema[ruleID].Items.Properties = subResultSchema
					}
				}
			case oldSchemaField != nil && newSchemaField == nil:
				return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, "any")
			case oldSchemaField == nil && newSchemaField != nil:
				return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, "any", newSchemaField.Items.Type)
			case oldSchemaField == nil && newSchemaField == nil:
			}
			continue
		}
		resultSchema[ruleID] = newSchemaField
	}
	return resultSchema, nil
}
