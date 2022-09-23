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

package cache

import (
	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
	"time"
)

func TestExpiration(t *testing.T) {
	c := NewCache(20, false)
	defer c.Close()
	clock := conf.Clock.(*clock.Mock)
	expects := [][]api.SourceTuple{
		{api.NewDefaultSourceTuple(map[string]interface{}{"a": 1}, nil)},
		{api.NewDefaultSourceTuple(map[string]interface{}{"a": 2}, nil), api.NewDefaultSourceTuple(map[string]interface{}{"a": 3}, nil)},
		{},
	}
	c.Set("a", expects[0])
	clock.Add(10 * time.Second)
	c.Set("b", expects[1])
	c.Set("c", expects[2])
	clock.Add(1 * time.Second)
	r1, ok := c.Get("a")
	if !ok {
		t.Error("a should exist")
		return
	}
	if !reflect.DeepEqual(r1, expects[0]) {
		t.Errorf("expect %v but get %v", expects[0], r1)
	}
	r2, ok := c.Get("b")
	if !ok {
		t.Error("b should exist")
		return
	}
	if !reflect.DeepEqual(r2, expects[1]) {
		t.Errorf("expect %v but get %v", expects[1], r2)
	}
	_, ok = c.Get("c")
	if ok {
		t.Error("c should not exist")
		return
	}

	clock.Add(10 * time.Second)
	r1, ok = c.Get("a")
	if ok {
		t.Error("a should not exist after expiration")
		return
	}
	r2, ok = c.Get("b")
	if !ok {
		t.Error("b should exist")
		return
	}
	if !reflect.DeepEqual(r2, expects[1]) {
		t.Errorf("expect %v but get %v", expects[1], r2)
	}
	clock.Add(10 * time.Second)
	r2, ok = c.Get("b")
	if ok {
		t.Error("b should not exist after expiration")
		return
	}
}

func TestNoExpiration(t *testing.T) {
	c := NewCache(0, true)
	defer c.Close()
	clock := conf.Clock.(*clock.Mock)
	expects := [][]api.SourceTuple{
		{api.NewDefaultSourceTuple(map[string]interface{}{"a": 1}, nil)},
		{api.NewDefaultSourceTuple(map[string]interface{}{"a": 2}, nil), api.NewDefaultSourceTuple(map[string]interface{}{"a": 3}, nil)},
		{},
	}
	c.Set("a", expects[0])
	clock.Add(10 * time.Second)
	c.Set("b", expects[1])
	c.Set("c", expects[2])

	clock.Add(100 * time.Second)
	r1, ok := c.Get("a")
	if !ok {
		t.Error("a should exist")
		return
	}
	if !reflect.DeepEqual(r1, expects[0]) {
		t.Errorf("expect %v but get %v", expects[0], r1)
	}
	r2, ok := c.Get("b")
	if !ok {
		t.Error("b should exist")
		return
	}
	if !reflect.DeepEqual(r2, expects[1]) {
		t.Errorf("expect %v but get %v", expects[1], r2)
	}
	_, ok = c.Get("c")
	if !ok {
		t.Error("c should exist")
		return
	}
}
