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

package context

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestState(t *testing.T) {
	err := store.SetupDefault()
	if err != nil {
		t.Error(err)
	}
	var (
		i      = 0
		ruleId = "testStateRule"
		value1 = 21
		value2 = "hello"
		value3 = "world"
		s      = map[string]interface{}{
			"key1": 21,
			"key3": "world",
		}
	)
	//initialization
	cStore, err := state.CreateStore(ruleId, api.AtLeastOnce)
	if err != nil {
		t.Errorf("Get store for rule %s error: %s", ruleId, err)
		return
	}
	ctx := Background().WithMeta("testStateRule", "op1", cStore).(*DefaultContext)
	defer cleanStateData()
	// Do state function
	_ = ctx.IncrCounter("key1", 20)
	_ = ctx.IncrCounter("key1", 1)
	v, err := ctx.GetCounter("key1")
	if err != nil {
		t.Errorf("%d.Get counter error: %s", i, err)
		return
	}
	if !reflect.DeepEqual(value1, v) {
		t.Errorf("%d.Get counter\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, value1, v)
	}
	err = ctx.PutState("key2", value2)
	if err != nil {
		t.Errorf("%d.Put state key2 error: %s", i, err)
		return
	}
	err = ctx.PutState("key3", value3)
	if err != nil {
		t.Errorf("%d.Put state key3 error: %s", i, err)
		return
	}
	v2, err := ctx.GetState("key2")
	if err != nil {
		t.Errorf("%d.Get state key2 error: %s", i, err)
		return
	}
	if !reflect.DeepEqual(value2, v2) {
		t.Errorf("%d.Get state\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, value2, v2)
	}
	err = ctx.DeleteState("key2")
	if err != nil {
		t.Errorf("%d.Delete state key2 error: %s", i, err)
		return
	}
	err = ctx.Snapshot()
	if err != nil {
		t.Errorf("%d.Snapshot error: %s", i, err)
		return
	}
	rs := ctx.snapshot
	if !reflect.DeepEqual(s, rs) {
		t.Errorf("%d.Snapshot\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, s, rs)
	}
}

func cleanStateData() {
	dbDir, err := conf.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}
	c := path.Join(dbDir, state.CheckpointListKey)
	err = os.RemoveAll(c)
	if err != nil {
		conf.Log.Error(err)
	}
}

func TestParseJsonPath(t *testing.T) {
	var tests = []struct {
		j string
		v []interface{} // values
		r []interface{} // parsed results
	}{
		{
			j: "$.a",
			v: []interface{}{
				map[string]interface{}{
					"a": 123,
					"b": "dafds",
				},
				map[string]interface{}{
					"a": "single",
					"c": 20.2,
				},
				map[string]interface{}{
					"b": "b",
					"c": "c",
				},
			},
			r: []interface{}{
				123,
				"single",
				nil,
			},
		}, {
			j: "$[0].a",
			v: []interface{}{
				[]map[string]interface{}{{
					"a": 123,
					"b": "dafds",
				}},
				[]map[string]interface{}{},
				[]map[string]interface{}{
					{
						"a": "single",
						"c": 20.2,
					},
					{
						"b": "b",
						"c": "c",
					},
				},
			},
			r: []interface{}{
				123,
				nil,
				"single",
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	ctx := Background().WithMeta("testStateRule", "op1", &state.MemoryStore{})
	for i, tt := range tests {
		var result []interface{}
		for _, v := range tt.v {
			prop, err := ctx.ParseJsonPath(tt.j, v)
			if err != nil {
				fmt.Printf("%d:%s parse %v error\n", i, tt.j, v)
			}
			result = append(result, prop)
		}
		if !reflect.DeepEqual(tt.r, result) {
			t.Errorf("%d. %s\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.j, tt.r, result)
		}
	}
}

func TestParseTemplate(t *testing.T) {
	var tests = []struct {
		j string
		v []interface{} // values
		r []interface{} // parsed results
	}{
		{
			j: "devices/{{.a}}",
			v: []interface{}{
				map[string]interface{}{
					"a": 123,
					"b": "dafds",
				},
				map[string]interface{}{
					"a": "single",
					"c": 20.2,
				},
				map[string]interface{}{
					"b": "b",
					"c": "c",
				},
			},
			r: []interface{}{
				"devices/123",
				"devices/single",
				"devices/<no value>",
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	ctx := Background().WithMeta("testStateRule", "op1", &state.MemoryStore{})
	for i, tt := range tests {
		var result []interface{}
		for _, v := range tt.v {
			prop, err := ctx.ParseTemplate(tt.j, v)
			if err != nil {
				fmt.Printf("%d:%s parse %v error\n", i, tt.j, v)
			}
			result = append(result, prop)
		}
		if !reflect.DeepEqual(tt.r, result) {
			t.Errorf("%d. %s\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.j, tt.r, result)
		}
	}
}

func TestTransition(t *testing.T) {
	var mockFunc transform.TransFunc = func(d interface{}) ([]byte, bool, error) {
		return []byte(fmt.Sprintf("%v", d)), true, nil
	}
	var tests = []struct {
		data interface{}
		r    []byte
	}{
		{
			data: "hello",
			r:    []byte(`hello`),
		}, {
			data: "world",
			r:    []byte(`world`),
		}, {
			data: map[string]interface{}{"a": "hello"},
			r:    []byte(`map[a:hello]`),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	ctx := Background().WithMeta("testTransRule", "op1", &state.MemoryStore{}).(*DefaultContext)
	nc := WithValue(ctx, internal.TransKey, mockFunc)
	for i, tt := range tests {
		r, _, _ := nc.TransformOutput(tt.data)
		if !reflect.DeepEqual(tt.r, r) {
			t.Errorf("%d\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, string(tt.r), string(r))
		}
	}
}
