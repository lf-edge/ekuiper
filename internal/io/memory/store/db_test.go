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
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestTable(t *testing.T) {
	tb := createTable("topicT", "a")
	tb.add(&xsql.Tuple{Message: map[string]interface{}{"a": 1, "b": "0"}})
	tb.add(&xsql.Tuple{Message: map[string]interface{}{"a": 2, "b": "0"}})
	tb.add(&xsql.Tuple{Message: map[string]interface{}{"a": 3, "b": "4"}})
	tb.add(&xsql.Tuple{Message: map[string]interface{}{"a": 1, "b": "1"}})
	v, _ := tb.Read([]string{"a"}, []interface{}{1})
	exp := []pubsub.MemTuple{
		&xsql.Tuple{Message: map[string]interface{}{"a": 1, "b": "1"}},
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read a 1 expect %v, but got %v", exp, v)
		return
	}
	v, _ = tb.Read([]string{"b"}, []interface{}{"0"})
	exp = []pubsub.MemTuple{
		&xsql.Tuple{Message: map[string]interface{}{"a": 2, "b": "0"}},
	}
	if !reflect.DeepEqual(v, exp) {
		t.Errorf("read b 0 expect %v, but got %v", exp, v)
		return
	}
	tb.add(&xsql.Tuple{Message: map[string]interface{}{"a": 5, "b": "0"}})
	tb.delete(3)
	tb.add(&xsql.Tuple{Message: map[string]interface{}{"a": 1, "b": "1"}})
	v, _ = tb.Read([]string{"b"}, []interface{}{"0"})
	exp = []pubsub.MemTuple{
		&xsql.Tuple{Message: map[string]interface{}{"a": 2, "b": "0"}},
		&xsql.Tuple{Message: map[string]interface{}{"a": 5, "b": "0"}},
	}
	if len(v) != 2 {
		t.Errorf("read 1 again expect %v, but got %v", exp, v)
		return
	} else {
		vv, _ := v[0].Value("a", "")
		if vv != 2 {
			v[0], v[1] = v[1], v[0]
		}
		if !reflect.DeepEqual(v, exp) {
			t.Errorf("read 1 again expect %v, but got %v", exp, v)
			return
		}
	}

	v, _ = tb.Read([]string{"a", "b"}, []interface{}{1, "1"})
	exp = []pubsub.MemTuple{
		&xsql.Tuple{Message: map[string]interface{}{"a": 1, "b": "1"}},
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
	tb.delete(1)
	v, _ = tb.Read([]string{"a"}, []interface{}{1})
	if v != nil {
		t.Errorf("read a 1 expect nil, but got %v", v)
	}
}

func TestDb(t *testing.T) {
	db = &database{
		tables: make(map[string]*tableCount),
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
	_, ok = db.getTable("t3", "a")
	if ok {
		t.Errorf("table t1 c should not exist")
		return
	}
	tc := db.tables["t1_a"]
	if tc.count != 2 {
		t.Errorf("table t1 a should have 2 instances but got %d", tc.count)
		return
	}
	tc = db.tables["t2_a"]
	if tc.count != 1 {
		t.Errorf("table t2 a should have 1 instances")
		return
	}
	db.dropTable("t1", "a")
	db.dropTable("t2", "a")
	_, ok = db.getTable("t2", "a")
	if ok {
		t.Errorf("table t2 a should not exist")
		return
	}
	tc = db.tables["t1_a"]
	if tc.count != 1 {
		t.Errorf("table t1 a should have 1 instances but got %d", tc.count)
		return
	}
}
