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

package schema

import (
	"sync"

	"github.com/lf-edge/ekuiper/v2/internal/converter/merge"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type SchemaLayer struct {
	sync.RWMutex
	schema map[string]*ast.JsonStreamField
	// ruleID -> schema
	schemaMap map[string]map[string]*ast.JsonStreamField
	// ruleID -> StreamName
	streamMap map[string]string
	// ruleID -> wildcard
	wildcardMap map[string]struct{}
}

func NewSchemaLayer(ruleID, streamName string, schema map[string]*ast.JsonStreamField, isWildcard bool) *SchemaLayer {
	s := &SchemaLayer{
		schemaMap:   make(map[string]map[string]*ast.JsonStreamField),
		streamMap:   make(map[string]string),
		wildcardMap: make(map[string]struct{}),
	}
	if schema != nil || isWildcard {
		s.schemaMap[ruleID] = schema
		s.streamMap[ruleID] = streamName
		if isWildcard {
			s.wildcardMap[ruleID] = struct{}{}
		}
		s.schema = schema
		s.storeSchema()
	}
	return s
}

func (s *SchemaLayer) storeSchema() {
	if len(s.wildcardMap) > 0 {
		for ruleID := range s.schemaMap {
			merge.AddRuleSchema(ruleID, s.streamMap[ruleID], nil, true)
		}
		return
	}
	for ruleID := range s.schemaMap {
		merge.AddRuleSchema(ruleID, s.streamMap[ruleID], s.schema, false)
	}
}

func (s *SchemaLayer) MergeSchema(ruleID, dataSource string, newSchema map[string]*ast.JsonStreamField, isWildcard bool) error {
	s.Lock()
	defer s.Unlock()
	delete(s.wildcardMap, ruleID)
	_, ok := s.schemaMap[ruleID]
	if ok {
		return nil
	}
	s.schemaMap[ruleID] = newSchema
	s.streamMap[ruleID] = dataSource
	if isWildcard {
		s.wildcardMap[ruleID] = struct{}{}
	} else {
		mergedSchema, err := mergeSchema(s.schema, newSchema)
		if err != nil {
			return err
		}
		s.schema = mergedSchema
	}
	s.storeSchema()
	return nil
}

func (s *SchemaLayer) DetachSchema(ruleID string) error {
	var err error
	s.Lock()
	defer s.Unlock()
	_, ok := s.schemaMap[ruleID]
	if ok {
		merge.RemoveRuleSchema(ruleID)
		delete(s.streamMap, ruleID)
		delete(s.wildcardMap, ruleID)
		delete(s.schemaMap, ruleID)
		newSchema := make(map[string]*ast.JsonStreamField)
		for _, schema := range s.schemaMap {
			newSchema, err = mergeSchema(newSchema, schema)
			if err != nil {
				return err
			}
		}
		s.schema = newSchema
		s.storeSchema()
	}
	return nil
}

func (s *SchemaLayer) GetSchema() map[string]*ast.JsonStreamField {
	s.RLock()
	defer s.RUnlock()
	if len(s.wildcardMap) > 0 {
		return nil
	}
	return s.schema
}

func mergeSchema(originSchema, newSchema map[string]*ast.JsonStreamField) (map[string]*ast.JsonStreamField, error) {
	return merge.MergeSchema(originSchema, newSchema)
}
