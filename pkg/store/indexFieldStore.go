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

package store

type IndexField struct {
	IndexFieldName           string      `json:"indexField"`
	IndexFieldValue          interface{} `json:"indexValue"`
	IndexFieldDataType       string      `json:"indexFieldType"`
	IndexFieldDateTimeFormat string      `json:"dateTimeFormat"`
}

type IndexFieldStore struct {
	IndexFieldValueList []*IndexField          `json:"indexFieldValueList"`
	IndexFieldValueMap  map[string]*IndexField `json:"indexFieldValueMap"`
}

func (store *IndexFieldStore) Init(name string, value interface{}, dataType string, format string) {
	store.IndexFieldValueList = make([]*IndexField, 0)
	store.IndexFieldValueMap = make(map[string]*IndexField)
	if name != "" {
		w := &IndexField{
			IndexFieldName:           name,
			IndexFieldValue:          value,
			IndexFieldDataType:       dataType,
			IndexFieldDateTimeFormat: format,
		}
		store.IndexFieldValueList = append(store.IndexFieldValueList, w)
		store.IndexFieldValueMap[name] = w
	}
}

func (store *IndexFieldStore) GetFieldList() []*IndexField {
	return store.IndexFieldValueList
}

func (store *IndexFieldStore) GetFieldMap() map[string]*IndexField {
	return store.IndexFieldValueMap
}

func (store *IndexFieldStore) UpdateFieldValue(name string, value interface{}) {
	w, ok := store.IndexFieldValueMap[name]
	if !ok {
		return
	}
	w.IndexFieldValue = value
}

func (store *IndexFieldStore) LoadFromList() {
	store.IndexFieldValueMap = make(map[string]*IndexField)
	for _, field := range store.GetFieldList() {
		store.IndexFieldValueMap[field.IndexFieldName] = field
	}
}
