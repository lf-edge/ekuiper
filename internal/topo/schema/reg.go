// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type SchemaStore struct {
	sync.RWMutex
	// rule -> datasource -> schema
	schemaMap map[string]map[string]map[string]*ast.JsonStreamField
	// rule -> datasource -> schema
	wildcardMap map[string]map[string]bool
	// shared stream schema reg, will be used by planner; stream -> sharelayer
	streamMap map[string]SchemaContainer
}

type SchemaContainer interface {
	GetSchema() map[string]*ast.JsonStreamField
	GetSchemaIndex() map[string]int
}

type schemaWrapper map[string]*ast.JsonStreamField

func (s schemaWrapper) GetSchemaIndex() map[string]int {
	temp := make(map[string]int, len(s))
	for k, v := range s {
		temp[k] = v.Index
	}
	return temp
}

func (s schemaWrapper) GetSchema() map[string]*ast.JsonStreamField {
	return s
}

func initStore() *SchemaStore {
	return &SchemaStore{
		schemaMap:   make(map[string]map[string]map[string]*ast.JsonStreamField),
		wildcardMap: make(map[string]map[string]bool),
		streamMap:   make(map[string]SchemaContainer),
	}
}

var GlobalSchemaStore = initStore()

type RuleSchemaResponse struct {
	// streamName -> schema
	Schema map[string]map[string]*ast.JsonStreamField `json:"schema"`
	// streamName -> wildcard
	Wildcard map[string]bool `json:"wildcard"`
}

func GetRuleSchema(ruleID string) RuleSchemaResponse {
	GlobalSchemaStore.RLock()
	defer GlobalSchemaStore.RUnlock()
	return RuleSchemaResponse{
		Schema:   GlobalSchemaStore.schemaMap[ruleID],
		Wildcard: GlobalSchemaStore.wildcardMap[ruleID],
	}
}

func AddRuleSchema(ruleID, dataSource string, schema map[string]*ast.JsonStreamField, isWildcard bool) {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	if _, ok := GlobalSchemaStore.schemaMap[ruleID]; !ok {
		GlobalSchemaStore.schemaMap[ruleID] = make(map[string]map[string]*ast.JsonStreamField)
	}
	if _, ok := GlobalSchemaStore.wildcardMap[ruleID]; !ok {
		GlobalSchemaStore.wildcardMap[ruleID] = make(map[string]bool)
	}

	if !isWildcard {
		GlobalSchemaStore.schemaMap[ruleID][dataSource] = schema
		GlobalSchemaStore.wildcardMap[ruleID][dataSource] = false
		return
	}
	GlobalSchemaStore.schemaMap[ruleID][dataSource] = nil
	GlobalSchemaStore.wildcardMap[ruleID][dataSource] = true
}

func RemoveRuleSchema(ruleID string) {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	delete(GlobalSchemaStore.schemaMap, ruleID)
	delete(GlobalSchemaStore.wildcardMap, ruleID)
}

func GetStream(name string) SchemaContainer {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	stream, ok := GlobalSchemaStore.streamMap[name]
	if !ok {
		stream = newSharedLayer()
		GlobalSchemaStore.streamMap[name] = stream
	}
	return stream
}

func GetStreamSchema(name string) (map[string]*ast.JsonStreamField, error) {
	GlobalSchemaStore.RLock()
	c, ok := GlobalSchemaStore.streamMap[name]
	GlobalSchemaStore.RUnlock()
	if !ok {
		return nil, nil
	}
	return c.GetSchema(), nil
}

func GetStreamSchemaIndex(streamName string) map[string]int {
	GlobalSchemaStore.RLock()
	c, ok := GlobalSchemaStore.streamMap[streamName]
	GlobalSchemaStore.RUnlock()
	if !ok {
		return nil
	}
	return c.GetSchemaIndex()
}

func AddStaticStream(streamName string, schema map[string]*ast.JsonStreamField) {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	GlobalSchemaStore.streamMap[streamName] = schemaWrapper(schema)
}

func RemoveStreamSchema(streamName string) {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	delete(GlobalSchemaStore.streamMap, streamName)
}
