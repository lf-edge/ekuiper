// Copyright 2022 EMQ Technologies Co., Ltd.
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
		fmt.Errorf("Table count is less than 0: %d", tc.count)
	}
	return tc.count
}

type database struct {
	sync.RWMutex
	tables map[string]*tableCount // topic: table
}

// getTable return the table of the topic.
func (db *database) getTable(topic string) (*Table, bool) {
	db.RLock()
	defer db.RUnlock()
	tc, ok := db.tables[topic]
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
func (db *database) addTable(topic string, keys []string) (*Table, bool) {
	db.Lock()
	defer db.Unlock()
	tc, ok := db.tables[topic]
	if ok {
		tc.Increase()
	} else {
		t := createTable(keys)
		tc = &tableCount{
			count: 1,
			t:     t,
		}
		db.tables[topic] = tc
	}
	return tc.t, !ok
}

// dropTable drop the table of the topic/values
// stops to accumulate job
// deletes the cache data
func (db *database) dropTable(topic string) error {
	db.Lock()
	defer db.Unlock()
	if tc, ok := db.tables[topic]; ok {
		if tc.Decrease() == 0 {
			if tc.t != nil && tc.t.cancel != nil {
				tc.t.cancel()
			}
			delete(db.tables, topic)
		}
		return nil
	}
	return fmt.Errorf("Table %s not found", topic)
}

// Table has one writer and multiple reader
type Table struct {
	sync.RWMutex
	// datamap is the overall data
	datamap  []api.SourceTuple
	hasIndex bool
	// indexes is the indexed data
	indexes map[string]map[interface{}][]api.SourceTuple
	cancel  context.CancelFunc
}

func createTable(keys []string) *Table {
	t := &Table{}
	if len(keys) > 0 {
		t.indexes = make(map[string]map[interface{}][]api.SourceTuple, len(keys))
		for _, k := range keys {
			t.indexes[k] = make(map[interface{}][]api.SourceTuple)
		}
		t.hasIndex = true
	}
	return t
}

func (t *Table) add(value api.SourceTuple) {
	t.Lock()
	defer t.Unlock()
	t.datamap = append(t.datamap, value)
	for k, v := range t.indexes {
		if val, ok := value.Message()[k]; ok {
			if _, ok := v[val]; !ok {
				v[val] = make([]api.SourceTuple, 0)
			}
			v[val] = append(v[val], value)
		}
	}
}

func (t *Table) delete(key string, value api.SourceTuple) error {
	v, ok := value.Message()[key]
	if !ok {
		return fmt.Errorf("value not found for key %s", key)
	}
	t.Lock()
	defer t.Unlock()
	if d, ok := t.indexes[key]; ok {
		if _, kok := d[v]; kok {
			delete(d, v)
		} else {
			// has index but not hit, so just return
			return nil
		}
	}
	// After delete index, also delete in the data
	arr := make([]api.SourceTuple, 0, len(t.datamap))
	for _, st := range t.datamap {
		if val, ok := st.Message()[key]; ok && val == v {
			for k, d := range t.indexes {
				if kval, ok := st.Message()[k]; ok {
					newarr := make([]api.SourceTuple, 0, len(d[kval]))
					for _, tuple := range d[kval] {
						if tv, ok := tuple.Message()[key]; ok && tv == v {
							continue
						}
						newarr = append(newarr, tuple)
					}
					d[kval] = newarr
				}
			}
			continue
		}
		arr = append(arr, st)
	}
	t.datamap = arr
	return nil
}

func (t *Table) Read(keys []string, values []interface{}) ([]api.SourceTuple, error) {
	t.RLock()
	defer t.RUnlock()
	data := t.datamap
	excludeKey := -1
	if t.hasIndex {
		// Find the first indexed key
		for i, k := range keys {
			if d, ok := t.indexes[k]; ok {
				data = d[values[i]]
				excludeKey = i
			}
		}
	}
	var result []api.SourceTuple
	for _, v := range data {
		match := true
		for i, k := range keys {
			if i == excludeKey {
				continue
			}
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
