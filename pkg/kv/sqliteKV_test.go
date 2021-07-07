// Copyright 2021 EMQ Technologies Co., Ltd.
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

package kv

import (
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSqliteKVStore_Funcs(t *testing.T) {
	abs, _ := filepath.Abs("test")
	if f, _ := os.Stat(abs); f != nil {
		os.Remove(abs)
	}

	ks := GetSqliteKVStore(abs)
	if e := ks.Open(); e != nil {
		t.Errorf("Failed to open data %s.", e)
	}

	if err := ks.Setnx("foo", "bar"); nil != err {
		t.Error(err)
	}

	var v string
	if ok, _ := ks.Get("foo", &v); ok {
		if !reflect.DeepEqual("bar", v) {
			t.Error("expect:bar", "get:", v)
		}
	} else {
		t.Errorf("Should not find the foo key.")
	}

	if err := ks.Setnx("foo1", "bar1"); nil != err {
		t.Error(err)
	}

	if err := ks.Set("foo1", "bar2"); nil != err {
		t.Error(err)
	}

	var v1 string
	if ok, _ := ks.Get("foo1", &v1); ok {
		if !reflect.DeepEqual("bar2", v1) {
			t.Error("expect:bar2", "get:", v1)
		}
	} else {
		t.Errorf("Should not find the foo1 key.")
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		if !reflect.DeepEqual(2, len(keys)) {
			t.Error("expect:2", "get:", len(keys))
		}
	}

	if e2 := ks.Close(); e2 != nil {
		t.Errorf("Failed to close data: %s.", e2)
	}

	if err := ks.Open(); nil != err {
		t.Error(err)
	}

	var v2 string
	if ok, _ := ks.Get("foo", &v2); ok {
		if !reflect.DeepEqual("bar", v2) {
			t.Error("expect:bar", "get:", v)
		}
	} else {
		t.Errorf("Should not find the foo key.")
	}

	if err := ks.Delete("foo1"); nil != err {
		t.Error(err)
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		reflect.DeepEqual(1, len(keys))
	}

	if err := ks.Clean(); nil != err {
		t.Error(err)
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		reflect.DeepEqual(0, len(keys))
	}

	dir, _ := filepath.Split(abs)
	abs = path.Join(dir, "sqliteKV.db")
	os.Remove(abs)

}
