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
	tables map[string]map[string]*tableCount // topic: index: []value
}

// getTable return the table of the topic/values.
// The second bool indicates if the topic exists
func (db *database) getTable(topic string, key string) (*Table, bool) {
	db.RLock()
	defer db.RUnlock()
	r, ok := db.tables[topic]
	if !ok {
		return nil, false
	}
	tc, ok := r[key]
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
	r, ok := db.tables[topic]
	if !ok {
		r = make(map[string]*tableCount)
		db.tables[topic] = r
	}
	tc, ok := r[key]
	if ok {
		tc.Increase()
	} else {
		t := createTable()
		tc = &tableCount{
			count: 1,
			t:     t,
		}
		r[key] = tc
	}
	return tc.t, !ok
}

// dropTable drop the table of the topic/values
// stops to accumulate job
// deletes the cache data
func (db *database) dropTable(topic string, key string) error {
	db.Lock()
	defer db.Unlock()
	if r, ok := db.tables[topic]; ok {
		if tc, tok := r[key]; tok {
			if tc.Decrease() == 0 {
				if tc.t != nil && tc.t.cancel != nil {
					tc.t.cancel()
				}
				delete(r, key)
			}
			return nil
		}
	}
	return fmt.Errorf("Table %s/%s not found", topic, key)
}

// Table has one writer and multiple reader
type Table struct {
	sync.RWMutex
	datamap map[string][]api.SourceTuple
	cancel  context.CancelFunc
}

func createTable() *Table {
	return &Table{
		datamap: make(map[string][]api.SourceTuple),
	}
}

func (t *Table) add(key string, value api.SourceTuple) {
	t.Lock()
	defer t.Unlock()
	t.datamap[key] = append(t.datamap[key], value)
}

func (t *Table) Read(values []interface{}) ([]api.SourceTuple, error) {
	t.RLock()
	defer t.RUnlock()
	mapkey := ""
	for _, k := range values {
		mapkey += fmt.Sprintf("%v,", k)
	}
	return t.datamap[mapkey], nil
}

var (
	db = &database{
		tables: make(map[string]map[string]*tableCount),
	}
)
