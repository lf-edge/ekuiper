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
)

func TestReg(t *testing.T) {
	db = &database{
		tables: make(map[string]*tableCount),
	}
	reg1, err := Reg("test", nil, "a")
	if err != nil {
		t.Errorf("register test error: %v", err)
		return
	}
	_, err2 := Reg("test", nil, "a")
	if err2 != nil {
		t.Errorf("register test error: %v", err2)
		return
	}
	exp := map[string]*tableCount{
		"test_a": {
			count: 2,
			t:     reg1,
		},
	}
	if !reflect.DeepEqual(exp, db.tables) {
		t.Errorf("register expect %v, but got %v", exp, db.tables)
		return
	}
	Unreg("test", "a")
	exp = map[string]*tableCount{
		"test_a": {
			count: 1,
			t:     reg1,
		},
	}
	if !reflect.DeepEqual(exp, db.tables) {
		t.Errorf("unregister expect %v, but got %v", exp, db.tables)
		return
	}
}
