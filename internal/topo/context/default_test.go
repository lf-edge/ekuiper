// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestStateIncr(t *testing.T) {
	data, err := conf.GetDataLoc()
	if err != nil {
		t.Error(err)
	}
	err = store.SetupDefault(data)
	if err != nil {
		t.Error(err)
	}
	var (
		i      = 0
		ruleId = "testStateRule"
		value1 = 10
	)
	// initialization
	cStore, err := state.CreateStore(ruleId, def.AtLeastOnce)
	if err != nil {
		t.Errorf("Get store for rule %s error: %s", ruleId, err)
		return
	}
	ctx := Background().WithMeta("testStateRule", "op1", cStore).(*DefaultContext)
	defer cleanStateData()
	// Do state function
	for j := 0; j < value1; j++ {
		go ctx.IncrCounter("key1", 1)
	}
	<-time.After(time.Second)
	v, err := ctx.GetCounter("key1")
	if err != nil {
		t.Errorf("%d.Get counter error: %s", i, err)
		return
	}
	if !reflect.DeepEqual(value1, v) {
		t.Errorf("%d.Get counter\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, value1, v)
	}
}

func TestState(t *testing.T) {
	data, err := conf.GetDataLoc()
	if err != nil {
		t.Error(err)
	}
	err = store.SetupDefault(data)
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
	// initialization
	cStore, err := state.CreateStore(ruleId, def.AtLeastOnce)
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
	err = ctx.Snapshot(1)
	if err != nil {
		t.Errorf("%d.Snapshot error: %s", i, err)
		return
	}
	rs, err := checkpoint.DecodeState(ctx.checkpoints.states[1])
	if err != nil {
		t.Errorf("%d.Decode snapshot error: %s", i, err)
		return
	}
	if !reflect.DeepEqual(s, rs) {
		t.Errorf("%d.Snapshot\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, s, rs)
	}
}

func TestSnapshotIsolationAndCheckpointIDs(t *testing.T) {
	gob.Register([]int{})
	cStore := &frozenCaptureStore{}
	ctx := Background().WithMeta("snapshotRule", "op1", cStore).(*DefaultContext)
	live := map[string]interface{}{
		"nested": map[string]interface{}{"value": 1},
		"slice":  []int{1, 2, 3},
	}
	if err := ctx.PutState("mutable", live); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Snapshot(1); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Snapshot(1); err == nil {
		t.Fatal("duplicate checkpoint ID must fail")
	}

	live["nested"].(map[string]interface{})["value"] = 2
	live["slice"].([]int)[0] = 9
	first, err := checkpoint.DecodeState(ctx.checkpoints.states[1])
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := map[string]interface{}{
		"mutable": map[string]interface{}{
			"nested": map[string]interface{}{"value": 1},
			"slice":  []int{1, 2, 3},
		},
	}
	if !reflect.DeepEqual(wantFirst, first) {
		t.Fatalf("first snapshot mismatch: want %#v, got %#v", wantFirst, first)
	}

	if err := ctx.Snapshot(2); err != nil {
		t.Fatal(err)
	}
	if err := ctx.SaveSnapshot(1); err != nil {
		t.Fatal(err)
	}
	if err := ctx.SaveSnapshot(1); err == nil {
		t.Fatal("saving an unknown checkpoint ID must fail")
	}
	stored, ok := cStore.states.Load(int64(1))
	if !ok {
		t.Fatal("frozen state was not saved")
	}
	saved, err := checkpoint.DecodeState(stored.([]byte))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantFirst, saved) {
		t.Fatalf("saved snapshot mismatch: want %#v, got %#v", wantFirst, saved)
	}

	second, err := checkpoint.DecodeState(ctx.checkpoints.states[2])
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first, second) {
		t.Fatal("successive checkpoints must capture distinct state")
	}
}

func TestDerivedContextSharesCheckpointLifecycle(t *testing.T) {
	cStore := &frozenCaptureStore{}
	base := Background().WithMeta("rule", "op", cStore).(*DefaultContext)
	derived := base.WithRun(1).WithInstance(2).(*DefaultContext)
	if err := derived.PutState("count", 42); err != nil {
		t.Fatal(err)
	}
	if err := derived.Snapshot(10); err != nil {
		t.Fatal(err)
	}
	if err := base.SaveSnapshot(10); err != nil {
		t.Fatal(err)
	}
	raw, ok := cStore.states.Load(int64(10))
	if !ok {
		t.Fatal("derived context snapshot was not persisted by base context")
	}
	saved, err := checkpoint.DecodeState(raw.([]byte))
	if err != nil {
		t.Fatal(err)
	}
	if saved["count"] != 42 {
		t.Fatalf("unexpected derived context state: %#v", saved)
	}
}

type frozenCaptureStore struct {
	states sync.Map
}

func (s *frozenCaptureStore) SaveState(_ int64, _ string, _ map[string]interface{}) error {
	return nil
}

func (s *frozenCaptureStore) SaveFrozenState(checkpointID int64, _ string, state []byte) error {
	s.states.Store(checkpointID, append([]byte(nil), state...))
	return nil
}

func (s *frozenCaptureStore) SaveCheckpoint(_ int64) error {
	return nil
}

func (s *frozenCaptureStore) GetOpState(_ string) (*sync.Map, error) {
	return &sync.Map{}, nil
}

func (s *frozenCaptureStore) Clean() error {
	return nil
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
	tests := []struct {
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
	tests := []struct {
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

func TestRuleBackground(t *testing.T) {
	conf.InitConf()
	conf.Config.Basic.ResourceProfileConfig.Enable = true
	c := RuleBackground("test")
	ctx := pprof.WithLabels(context.Background(), pprof.Labels("rule", "test"))
	assert.Equal(t, c.ctx, ctx)
}
