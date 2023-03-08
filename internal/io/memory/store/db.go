// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
)

type tableCount struct {
	sync.RWMutex
	count int
	t     *Table
}

func (tc *tableCount) Increase() int {
	tc.Lock()
	defer tc.Unlock()
	tc.count++
	return tc.count
}

func (tc *tableCount) Decrease() int {
	tc.Lock()
	defer tc.Unlock()
	tc.count--
	if tc.count < 0 {
		conf.Log.Errorf("Table count is less than 0: %d", tc.count)
	}
	return tc.count
}

type database struct {
	sync.RWMutex
	tables map[string]*tableCount // topic_key: table
}

// getTable return the table of the topic.
func (db *database) getTable(topic string, key string) (*Table, bool) {
	db.RLock()
	defer db.RUnlock()
	tableId := fmt.Sprintf("%s_%s", topic, key)
	tc, ok := db.tables[tableId]
	if ok {
		return tc.t, true
	} else {
		return nil, false
	}
}

// addTable add a table to the database
// If the table already exists, return the existing table;
// otherwise, create a new table and return it.
// The second argument is to indicate if the table is newly created
func (db *database) addTable(topic string, key string) (*Table, bool) {
	db.Lock()
	defer db.Unlock()
	tableId := fmt.Sprintf("%s_%s", topic, key)
	tc, ok := db.tables[tableId]
	if ok {
		tc.Increase()
	} else {
		t := createTable(topic, key)
		tc = &tableCount{
			count: 1,
			t:     t,
		}
		db.tables[tableId] = tc
	}
	return tc.t, !ok
}

// dropTable drop the table of the topic/values
// stops to accumulate job
// deletes the cache data
func (db *database) dropTable(topic string, key string) error {
	tableId := fmt.Sprintf("%s_%s", topic, key)
	db.Lock()
	defer db.Unlock()
	if tc, ok := db.tables[tableId]; ok {
		if tc.Decrease() == 0 {
			if tc.t != nil && tc.t.cancel != nil {
				tc.t.cancel()
			}
			delete(db.tables, tableId)
		}
		return nil
	}
	return fmt.Errorf("Table %s not found", tableId)
}

// Table has one writer and multiple reader
type Table struct {
	sync.RWMutex
	topic string
	key   string
	// datamap is the overall data indexed by primary key
	datamap map[interface{}]api.SourceTuple
	cancel  context.CancelFunc
}

func createTable(topic string, key string) *Table {
	t := &Table{topic: topic, key: key, datamap: make(map[interface{}]api.SourceTuple)}
	return t
}

func (t *Table) add(value api.SourceTuple) {
	t.Lock()
	defer t.Unlock()
	keyval, ok := value.Message()[t.key]
	if !ok {
		conf.Log.Errorf("add to table %s omitted, value not found for key %s", t.topic, t.key)
	}
	t.datamap[keyval] = value
}

func (t *Table) delete(key interface{}) {
	t.Lock()
	defer t.Unlock()
	delete(t.datamap, key)
}

func (t *Table) Read(keys []string, values []interface{}) ([]api.SourceTuple, error) {
	t.RLock()
	defer t.RUnlock()
	// Find the primary key
	var matched api.SourceTuple
	for i, k := range keys {
		if k == t.key {
			matched = t.datamap[values[i]]
		}
	}
	if matched != nil {
		match := true
		for i, k := range keys {
			if val, ok := matched.Message()[k]; !ok || val != values[i] {
				match = false
				break
			}
		}
		if match {
			return []api.SourceTuple{matched}, nil
		} else {
			return nil, nil
		}
	}
	var result []api.SourceTuple
	for _, v := range t.datamap {
		match := true
		for i, k := range keys {
			if val, ok := v.Message()[k]; !ok || val != values[i] {
				match = false
				break
			}
		}
		if match {
			result = append(result, v)
		}
	}
	return result, nil
}

var (
	db = &database{
		tables: make(map[string]*tableCount),
	}
)
