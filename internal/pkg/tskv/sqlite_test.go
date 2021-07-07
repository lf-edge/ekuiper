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

package tskv

import (
	"reflect"
	"testing"
)

func TestSqlite_Funcs(t *testing.T) {
	ks, e := NewSqlite("test")
	if e != nil {
		t.Errorf("Failed to create tskv %s.", e)
		return
	}

	if ok, err := ks.Set(1000, "bar1"); nil != err {
		t.Error(err)
	} else if !ok {
		t.Error("should allow key 1000")
	}

	if ok, err := ks.Set(1500, "bar15"); nil != err {
		t.Error(err)
	} else if !ok {
		t.Error("should allow key 1500")
	}

	if ok, err := ks.Set(2000, "bar2"); nil != err {
		t.Error(err)
	} else if !ok {
		t.Error("should allow key 2000")
	}

	if ok, err := ks.Set(3000, "bar3"); nil != err {
		t.Error(err)
	} else if !ok {
		t.Error("should allow key 3000")
	}

	if ok, err := ks.Set(2500, "bar25"); nil != err {
		t.Error(err)
	} else if ok {
		t.Error("should deny key 2500")
	}

	var v string
	if k, err := ks.Last(&v); err != nil {
		t.Error(err)
	} else if k != 3000 || v != "bar3" {
		t.Errorf("Last expect 3000/bar3 but got %d/%s", k, v)
	}

	if ok, _ := ks.Get(2000, &v); ok {
		if !reflect.DeepEqual("bar2", v) {
			t.Error("expect:bar", "get:", v)
		}
	} else {
		t.Errorf("Should find key 2000.")
	}

	if err := ks.Delete(1500); nil != err {
		t.Error(err)
	}

	if ok, _ := ks.Get(1500, &v); ok {
		t.Errorf("Should not find deleted key 1500.")
	}

	if ok, err := ks.Set(3500, "bar35"); nil != err {
		t.Error(err)
	} else if !ok {
		t.Error("should allow key 3500")
	}

	if err := ks.DeleteBefore(3000); nil != err {
		t.Error(err)
	}

	if ok, _ := ks.Get(1000, &v); ok {
		t.Errorf("Should not find deleted key 1000.")
	}

	if ok, _ := ks.Get(2000, &v); ok {
		t.Errorf("Should not find deleted key 2000.")
	}

	if ok, _ := ks.Get(3000, &v); ok {
		if !reflect.DeepEqual("bar3", v) {
			t.Error("expect:bar3", "get:", v)
		}
	} else {
		t.Errorf("Should find key 3000.")
	}

	if ok, _ := ks.Get(3500, &v); ok {
		if !reflect.DeepEqual("bar35", v) {
			t.Error("expect:bar35", "get:", v)
		}
	} else {
		t.Errorf("Should find key 3500.")
	}

	if err := ks.Drop(); err != nil {
		t.Error(err)
	}
}
