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

//go:build redisdb || !core

package redis

import (
	"github.com/alicebob/miniredis/v2"
	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
)

var addr string

func init() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	addr = "localhost:" + s.Port()
	// Mock id key data
	s.Set("1", `{"id":1,"name":"John","address":34,"mobile":"334433"}`)
	s.Set("2", `{"id":2,"name":"Susan","address":22,"mobile":"666433"}`)
	// Mock group key list data
	s.Lpush("group1", `{"id":1,"name":"John"}`)
	s.Lpush("group1", `{"id":2,"name":"Susan"}`)
	s.Lpush("group2", `{"id":3,"name":"Nancy"}`)
	s.Lpush("group3", `{"id":4,"name":"Tom"}`)
}

// TestSingle test lookup value of a single map
func TestSingle(t *testing.T) {
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource()
	err := ls.Configure("0", map[string]interface{}{"addr": addr, "datatype": "string"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ls.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var tests = []struct {
		value  int
		result []api.SourceTuple
	}{
		{
			value: 1,
			result: []api.SourceTuple{
				api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(1), "name": "John", "address": float64(34), "mobile": "334433"}, nil),
			},
		}, {
			value: 2,
			result: []api.SourceTuple{
				api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(2), "name": "Susan", "address": float64(22), "mobile": "666433"}, nil),
			},
		}, {
			value:  3,
			result: []api.SourceTuple{},
		},
	}
	for i, tt := range tests {
		actual, err := ls.Lookup(ctx, []string{"id"}, []interface{}{tt.value})
		if err != nil {
			t.Errorf("Test %d: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(actual, tt.result) {
			t.Errorf("Test %d: expected %v, actual %v", i, tt.result, actual)
			continue
		}
	}
}

func TestList(t *testing.T) {
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource()
	err := ls.Configure("0", map[string]interface{}{"addr": addr, "datatype": "list"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ls.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var tests = []struct {
		value  string
		result []api.SourceTuple
	}{
		{
			value: "group1",
			result: []api.SourceTuple{
				api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(2), "name": "Susan"}, nil),
				api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(1), "name": "John"}, nil),
			},
		}, {
			value: "group2",
			result: []api.SourceTuple{
				api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(3), "name": "Nancy"}, nil),
			},
		}, {
			value:  "group4",
			result: []api.SourceTuple{},
		},
	}
	for i, tt := range tests {
		actual, err := ls.Lookup(ctx, []string{"id"}, []interface{}{tt.value})
		if err != nil {
			t.Errorf("Test %d: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(actual, tt.result) {
			t.Errorf("Test %d: expected %v, actual %v", i, tt.result, actual)
			continue
		}
	}
}
