// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/pkg/kv"
)

func TestKvSetnx(ks kv.KeyValue, t *testing.T) {
	if err := ks.Setnx("foo", "bar"); nil != err {
		t.Error(err)
	}
	var val string
	_, _ = ks.Get("foo", &val)
	if val != "bar" {
		t.Error("expect:bar", "get:", val)
	}

	if err := ks.Setnx("foo", "bar1"); nil == err {
		t.Errorf("Can't overwrite an existing intem: %v", err)
	}
}

func TestKvSetGet(ks kv.KeyValue, t *testing.T) {
	var val string
	// SetNX
	if err := ks.Setnx("foo", "bar"); nil != err {
		t.Error(err)
	}
	_, _ = ks.Get("foo", &val)
	if val != "bar" {
		t.Error("expect:bar", "get:", val)
	}

	if err := ks.Setnx("foo", "bar1"); nil == err {
		t.Errorf("Can't overwrite an existing intem: %v", err)
	}
	_, _ = ks.Get("foo", &val)
	if val != "bar" {
		t.Error("expect:bar", "get:", val)
	}

	// Set
	if err := ks.Set("foo", "bar"); nil != err {
		t.Error(err)
	}
	_, _ = ks.Get("foo", &val)
	if val != "bar" {
		t.Error("expect:bar", "get:", val)
	}

	if err := ks.Set("foo", "bar1"); nil != err {
		t.Errorf("Set should overwrite an existing record")
	}
	_, _ = ks.Get("foo", &val)
	if val != "bar1" {
		t.Error("expect:bar", "get:", val)
	}
}

func TestKvSet(ks kv.KeyValue, t *testing.T) {
	if err := ks.Set("foo", "bar"); nil != err {
		t.Error(err)
	}

	if err := ks.Set("foo", "bar1"); nil != err {
		t.Errorf("Set should overwrite an existing record")
	}
}

func TestKvGet(ks kv.KeyValue, t *testing.T) {
	if err := ks.Setnx("foo", "bar"); nil != err {
		t.Error(err)
	}

	var v string
	if ok, _ := ks.Get("foo", &v); ok {
		if !reflect.DeepEqual("bar", v) {
			t.Error("expect:bar", "get:", v)
		}
	} else {
		t.Errorf("Should find the foo key")
	}
}

func TestKvGetKeyedState(ks kv.KeyValue, t *testing.T) {
	if err := ks.SetKeyedState("foo", "bar"); nil != err {
		t.Error(err)
	}

	if v, err := ks.GetKeyedState("foo"); err != nil {
		t.Errorf("Should find the foo key")
	} else {
		if !reflect.DeepEqual("bar", v) {
			t.Error("expect:bar", "get:", v)
		}
	}
}

func TestKvKeys(length int, ks kv.KeyValue, t *testing.T) {
	expected := make([]string, 0)
	for i := 0; i < length; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := ks.Setnx(key, value); err != nil {
			t.Errorf("It should be set")
		}
		expected = append(expected, key)
	}

	var keys []string
	var err error
	if keys, err = ks.Keys(); err != nil {
		t.Errorf("Failed to get value: %s.", err)
	} else if !reflect.DeepEqual(length, len(keys)) {
		t.Errorf("expect: %d, got: %d", length, len(keys))
	}
	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("Keys do not match expected %s != %s", keys, expected)
	}
}

func TestKvAll(length int, ks kv.KeyValue, t *testing.T) {
	expected := make(map[string]string)
	for i := 0; i < length; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := ks.Setnx(key, value); err != nil {
			t.Errorf("It should be set")
		}
		expected[key] = value
	}

	var (
		all map[string]string
		err error
	)
	if all, err = ks.All(); err != nil {
		t.Errorf("Failed to get value: %s.", err)
		return
	} else if length != len(all) {
		t.Errorf("expect: %d, got: %d", length, len(all))
		return
	}
	if !reflect.DeepEqual(all, expected) {
		t.Errorf("All values do not match expected %s != %s", all, expected)
	}
}
