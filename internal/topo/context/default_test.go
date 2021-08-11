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

package context

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestState(t *testing.T) {
	err := kv.SetupDefault()
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
	store, err := state.CreateStore(ruleId, api.AtLeastOnce)
	if err != nil {
		t.Errorf("Get store for rule %s error: %s", ruleId, err)
		return
	}
	ctx := Background().WithMeta("testStateRule", "op1", store).(*DefaultContext)
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
