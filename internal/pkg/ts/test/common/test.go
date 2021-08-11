// Copyright 2021 INTECH Process Automation Ltd.
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

package common

import (
	"github.com/lf-edge/ekuiper/pkg/kv/stores"
	"reflect"
	"testing"
)

var (
	Keys   = []int64{1000, 1500, 2000, 3000}
	Values = []string{"bar1", "bar15", "bar2", "bar3"}
)

func TestTsSet(ks stores.Tskv, t *testing.T) {
	load(ks, t)

	if ok, err := ks.Set(2500, "bar25"); nil != err {
		t.Error(err)
	} else if ok {
		t.Errorf("should deny key 2500 while last one is 3000")
	}
}

func TestTsLast(ks stores.Tskv, t *testing.T) {
	load(ks, t)

	var v string
	if k, err := ks.Last(&v); err != nil {
		t.Error(err)
	} else if k != 3000 || v != "bar3" {
		t.Errorf("Last expect 3000/bar3 but got %d/%s", k, v)
	}
}

func TestTsGet(ks stores.Tskv, t *testing.T) {
	load(ks, t)

	var value string
	if ok, _ := ks.Get(2000, &value); ok {
		if !reflect.DeepEqual("bar2", value) {
			t.Error("expect:bar", "get:", value)
		}
	} else {
		t.Errorf("Should find key 2000.")
	}
}

func TestTsDelete(ks stores.Tskv, t *testing.T) {
	load(ks, t)

	if err := ks.Delete(1500); nil != err {
		t.Error(err)
	}

	var value string
	if ok, _ := ks.Get(1500, &value); ok {
		t.Errorf("Should not find deleted key 1500.")
	}
}

func TestTsDeleteBefore(ks stores.Tskv, t *testing.T) {
	load(ks, t)

	if ok, err := ks.Set(3500, "bar35"); nil != err {
		t.Error(err)
	} else if !ok {
		t.Error("should allow key 3500")
	}

	if err := ks.DeleteBefore(3000); nil != err {
		t.Error(err)
	}

	var value string
	if ok, _ := ks.Get(1000, &value); ok {
		t.Errorf("Should not find deleted key 1000.")
	}
	if ok, _ := ks.Get(2000, &value); ok {
		t.Errorf("Should not find deleted key 2000.")
	}

	if ok, _ := ks.Get(3000, &value); ok {
		if !reflect.DeepEqual("bar3", value) {
			t.Error("expect:bar3", "get:", value)
		}
	} else {
		t.Errorf("Should find key 3000.")
	}

	if ok, _ := ks.Get(3500, &value); ok {
		if !reflect.DeepEqual("bar35", value) {
			t.Error("expect:bar35", "get:", value)
		}
	} else {
		t.Errorf("Should find key 3500.")
	}
}

func load(ks stores.Tskv, t *testing.T) {
	for i := 0; i < len(Keys); i++ {
		k := Keys[i]
		v := Values[i]
		if ok, err := ks.Set(k, v); nil != err {
			t.Error(err)
		} else if !ok {
			t.Errorf("should allow key %d", k)
		}
	}
}
