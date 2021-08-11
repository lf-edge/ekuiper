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
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/kv/stores"
	"reflect"
	"testing"
)

func TestKvSetnx(ks stores.KeyValue, t *testing.T) {

	if err := ks.Setnx("foo", "bar"); nil != err {
		t.Error(err)
	}

	if err := ks.Setnx("foo", "bar1"); nil == err {
		t.Errorf("Can't overwrite an existing intem")
	}
}

func TestKvSet(ks stores.KeyValue, t *testing.T) {

	if err := ks.Set("foo", "bar"); nil != err {
		t.Error(err)
	}

	if err := ks.Set("foo", "bar1"); nil != err {
		t.Errorf("Set should overwrite an existing record")
	}
}

func TestKvGet(ks stores.KeyValue, t *testing.T) {

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

func TestKvKeys(length int, ks stores.KeyValue, t *testing.T) {

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
