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
	tb := createTable([]string{"a"})
	tb.add(api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "0"}, nil))
	tb.add(api.NewDefaultSourceTuple(map[string]interface{}{"a": 2, "b": "0"}, nil))
	tb.add(api.NewDefaultSourceTuple(map[string]interface{}{"a": 3, "b": "4"}, nil))
	tb.add(api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil))
	v, _ := tb.Read([]string{"a"}, []interface{}{1})
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "0"}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 1 expect %v, but got %v", exp, v)
		return
	}
	v, _ = tb.Read([]string{"a"}, []interface{}{3})
	exp = []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 3, "b": "4"}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 3 expect %v, but got %v", exp, v)
		return
	}
	tb.add(api.NewDefaultSourceTuple(map[string]interface{}{"a": 5, "b": "0"}, nil))
	tb.delete("b", api.NewDefaultSourceTuple(map[string]interface{}{"a": 3, "b": "4"}, nil))
	tb.add(api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil))
	v, _ = tb.Read([]string{"a"}, []interface{}{1})
	exp = []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "0"}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read 1 again expect %v, but got %v", exp, v)
		return
	}
	v, _ = tb.Read([]string{"a", "b"}, []interface{}{1, "1"})
	exp = []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil),
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read a,b expect %v, but got %v", exp, v)
		return
	}
	v, _ = tb.Read([]string{"a"}, []interface{}{3})
	if v != nil {
		t.Errorf("read a 3 expect nil, but got %v", v)
		return
	}
	tb.delete("a", api.NewDefaultSourceTuple(map[string]interface{}{"a": 1, "b": "1"}, nil))
	v, _ = tb.Read([]string{"a"}, []interface{}{1})
	if v != nil {
		t.Errorf("read a 1 expect nil, but got %v", v)
	}
}

func TestDb(t *testing.T) {
	db = &database{
		tables: make(map[string]*tableCount),
	}
	db.addTable("t1", []string{"a"})
	db.addTable("t1", []string{"a", "b"})
	db.addTable("t2", []string{"a"})
	db.addTable("t1", []string{"a"})
	_, ok := db.getTable("t1")
	if !ok {
		t.Errorf("table t1 a should exist")
		return
	}
	_, ok = db.getTable("t1")
	if !ok {
		t.Errorf("table t1 b should exist")
		return
	}
	_, ok = db.getTable("t3")
	if ok {
		t.Errorf("table t1 c should not exist")
		return
	}
	tc := db.tables["t1"]
	if tc.count != 3 {
		t.Errorf("table t1 a should have 2 instances but got %d", tc.count)
		return
	}
	tc = db.tables["t2"]
	if tc.count != 1 {
		t.Errorf("table t1 a should have 1 instances")
		return
	}
	db.dropTable("t1")
	db.dropTable("t2")
	_, ok = db.getTable("t2")
	if ok {
		t.Errorf("table ta a should not exist")
		return
	}
	tc = db.tables["t1"]
	if tc.count != 2 {
		t.Errorf("table t1 a should have 2 instances but got %d", tc.count)
		return
	}
}
