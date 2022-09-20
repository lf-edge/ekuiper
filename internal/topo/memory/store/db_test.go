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
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
)

func TestTable(t *testing.T) {
	tb := createTable()
	tb.add("1,", api.NewDefaultSourceTuple(map[string]interface{}{"a": 1}, nil))
	tb.add("2,", api.NewDefaultSourceTuple(map[string]interface{}{"a": 2}, nil))
	tb.add("3,", api.NewDefaultSourceTuple(map[string]interface{}{"a": 3}, nil))
	tb.add("1,", api.NewDefaultSourceTuple(map[string]interface{}{"a": 4}, nil))
	v, _ := tb.Read([]interface{}{"1"})
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 4}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 1 expect %v, but got %v", exp, v)
		return
	}
	v, _ = tb.Read([]interface{}{"3"})
	exp = []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 3}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 3 expect %v, but got %v", exp, v)
		return
	}
	tb.add("1,3,", api.NewDefaultSourceTuple(map[string]interface{}{"a": 5}, nil))
	tb.add("1,", api.NewDefaultSourceTuple(map[string]interface{}{"a": 6}, nil))
	v, _ = tb.Read([]interface{}{"1"})
	exp = []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 4}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 6}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 1 again expect %v, but got %v", exp, v)
		return
	}
	v, _ = tb.Read([]interface{}{"1", "3"})
	exp = []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 5}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 1,3 expect %v, but got %v", exp, v)
		return
	}
}

func TestDb(t *testing.T) {
	db = &database{
		tables: make(map[string]map[string]*tableCount),
	}
	db.addTable("t1", "a")
	db.addTable("t1", "b")
	db.addTable("t2", "a")
	db.addTable("t1", "a")
	_, ok := db.getTable("t1", "a")
	if !ok {
		t.Errorf("table t1 a should exist")
		return
	}
	_, ok = db.getTable("t1", "b")
	if !ok {
		t.Errorf("table t1 b should exist")
		return
	}
	_, ok = db.getTable("t1", "c")
	if ok {
		t.Errorf("table t1 c should not exist")
		return
	}
	tc := db.tables["t1"]["a"]
	if tc.count != 2 {
		t.Errorf("table t1 a should have 2 instances")
		return
	}
	tc = db.tables["t2"]["a"]
	if tc.count != 1 {
		t.Errorf("table t1 a should have 1 instances")
		return
	}
	db.dropTable("t1", "a")
	db.dropTable("t2", "a")
	_, ok = db.getTable("t2", "a")
	if ok {
		t.Errorf("table ta a should not exist")
		return
	}
	tc = db.tables["t1"]["a"]
	if tc.count != 1 {
		t.Errorf("table t1 a should have 2 instances")
		return
	}
}
