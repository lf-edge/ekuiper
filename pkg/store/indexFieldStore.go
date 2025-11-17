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

import "sync"

type IndexField struct {
	IndexFieldName           string      `json:"indexField"`
	IndexFieldValue          interface{} `json:"indexValue"`
	IndexFieldDataType       string      `json:"indexFieldType"`
	IndexFieldDateTimeFormat string      `json:"dateTimeFormat"`
}

type IndexFieldStoreWrap struct {
	// use mutex to modify value in future
	sync.RWMutex
	store *IndexFieldStore
}

type IndexFieldStore struct {
	IndexFieldValueList []*IndexField          `json:"indexFieldValueList"`
	IndexFieldValueMap  map[string]*IndexField `json:"indexFieldValueMap"`
}

func NewIndexFieldWrap(fields ...*IndexField) *IndexFieldStoreWrap {
	wrap := &IndexFieldStoreWrap{}
	wrap.store = &IndexFieldStore{}
	wrap.store.IndexFieldValueList = make([]*IndexField, 0)
	wrap.store.IndexFieldValueList = append(wrap.store.IndexFieldValueList, fields...)
	wrap.LoadFromList()
	return wrap
}

func (wrap *IndexFieldStoreWrap) InitByStore(store *IndexFieldStore) {
	wrap.store = store
}

func (wrap *IndexFieldStoreWrap) GetStore() *IndexFieldStore {
	wrap.RLock()
	defer wrap.RUnlock()
	return wrap.store
}

func (wrap *IndexFieldStoreWrap) Init(fields ...*IndexField) {
	store := &IndexFieldStore{}
	wrap.store = store
	store.IndexFieldValueList = make([]*IndexField, 0)
	store.IndexFieldValueMap = make(map[string]*IndexField)
	for _, field := range fields {
		if field.IndexFieldName == "" {
			continue
		}
		store.IndexFieldValueList = append(store.IndexFieldValueList, field)
		store.IndexFieldValueMap[field.IndexFieldName] = field
	}
}

func (wrap *IndexFieldStoreWrap) GetFieldList() []*IndexField {
	wrap.RLock()
	defer wrap.RUnlock()
	return wrap.store.IndexFieldValueList
}

func (wrap *IndexFieldStoreWrap) GetFieldMap() map[string]*IndexField {
	wrap.RLock()
	defer wrap.RUnlock()
	return wrap.store.IndexFieldValueMap
}

func (wrap *IndexFieldStoreWrap) UpdateFieldValue(name string, value interface{}) {
	wrap.Lock()
	defer wrap.Unlock()
	w, ok := wrap.store.IndexFieldValueMap[name]
	if !ok {
		return
	}
	w.IndexFieldValue = value
}

func (wrap *IndexFieldStoreWrap) UpdateByInput(input map[string]interface{}) {
	wrap.Lock()
	defer wrap.Unlock()
	for k, v := range input {
		w, ok := wrap.store.IndexFieldValueMap[k]
		if !ok {
			continue
		}
		w.IndexFieldValue = v
	}
}

func (wrap *IndexFieldStoreWrap) LoadFromList() {
	wrap.Lock()
	defer wrap.Unlock()
	wrap.store.IndexFieldValueMap = make(map[string]*IndexField)
	for _, field := range wrap.store.IndexFieldValueList {
		wrap.store.IndexFieldValueMap[field.IndexFieldName] = field
	}
}
