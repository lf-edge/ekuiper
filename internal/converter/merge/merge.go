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

package merge

import (
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type SchemaStore struct {
	sync.RWMutex
	// rule -> datasource -> schema
	SchemaMap map[string]map[string]map[string]*ast.JsonStreamField
	// rule -> datasource -> schema
	WildcardMap map[string]map[string]bool
}

var GlobalSchemaStore = &SchemaStore{}

func init() {
	GlobalSchemaStore.SchemaMap = make(map[string]map[string]map[string]*ast.JsonStreamField)
	GlobalSchemaStore.WildcardMap = make(map[string]map[string]bool)
}

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
		Schema:   GlobalSchemaStore.SchemaMap[ruleID],
		Wildcard: GlobalSchemaStore.WildcardMap[ruleID],
	}
}

func AddRuleSchema(ruleID, dataSource string, schema map[string]*ast.JsonStreamField, isWildcard bool) {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	if _, ok := GlobalSchemaStore.SchemaMap[ruleID]; !ok {
		GlobalSchemaStore.SchemaMap[ruleID] = make(map[string]map[string]*ast.JsonStreamField)
	}
	if _, ok := GlobalSchemaStore.WildcardMap[ruleID]; !ok {
		GlobalSchemaStore.WildcardMap[ruleID] = make(map[string]bool)
	}

	if !isWildcard {
		GlobalSchemaStore.SchemaMap[ruleID][dataSource] = schema
		GlobalSchemaStore.WildcardMap[ruleID][dataSource] = false
		return
	}
	GlobalSchemaStore.SchemaMap[ruleID][dataSource] = nil
	GlobalSchemaStore.WildcardMap[ruleID][dataSource] = true
}

func RemoveRuleSchema(ruleID string) {
	GlobalSchemaStore.Lock()
	defer GlobalSchemaStore.Unlock()
	delete(GlobalSchemaStore.SchemaMap, ruleID)
	delete(GlobalSchemaStore.WildcardMap, ruleID)
}

func MergeSchema(originSchema, newSchema map[string]*ast.JsonStreamField) (map[string]*ast.JsonStreamField, error) {
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
					subResultSchema, err := MergeSchema(oldSchemaField.Properties, newSchemaField.Properties)
					if err != nil {
						return nil, err
					}
					resultSchema[ruleID].Properties = subResultSchema
				case "array":
					if oldSchemaField.Items.Type != newSchemaField.Items.Type {
						return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, newSchemaField.Items.Type)
					}
					if oldSchemaField.Items.Type == "struct" {
						subResultSchema, err := MergeSchema(oldSchemaField.Items.Properties, newSchemaField.Items.Properties)
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
