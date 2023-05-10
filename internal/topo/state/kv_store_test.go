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

package state

import (
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"sync"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestLifecycle(t *testing.T) {
	var (
		i             = 0
		ruleId        = "test1"
		checkpointIds = []int64{1, 2, 3}
		opIds         = []string{"op1", "op2", "op3"}
		r             = map[string]interface{}{
			"1": map[string]interface{}{
				"op1": map[string]interface{}{
					"op": "op1",
					"oi": 0,
					"ci": 0,
				},
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 0,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 0,
				},
			},
			"2": map[string]interface{}{
				"op1": map[string]interface{}{
					"op": "op1",
					"oi": 0,
					"ci": 1,
				},
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 1,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 1,
				},
			},
			"3": map[string]interface{}{
				"op1": map[string]interface{}{
					"op": "op1",
					"oi": 0,
					"ci": 2,
				},
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 2,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 2,
				},
			},
		}
		rm = map[string]interface{}{
			"1": map[string]interface{}{
				"op1": map[string]interface{}{
					"op": "op1",
					"oi": 0,
					"ci": 0,
				},
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 0,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 0,
				},
			},
			"2": map[string]interface{}{
				"op1": map[string]interface{}{
					"op": "op1",
					"oi": 0,
					"ci": 1,
				},
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 1,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 1,
				},
			},
			"3": map[string]interface{}{
				"op1": map[string]interface{}{
					"op": "op1",
					"oi": 0,
					"ci": 2,
				},
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 2,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 2,
				},
			},
			"10000": map[string]interface{}{
				"op2": map[string]interface{}{
					"op": "op2",
					"oi": 1,
					"ci": 10000,
				},
				"op3": map[string]interface{}{
					"op": "op3",
					"oi": 2,
					"ci": 10000,
				},
			},
		}
	)
	func() {
		cleanStateData()
		err := store.SetupDefault()
		if err != nil {
			t.Error(err)
		}
		store, err := getKVStore(ruleId)
		if err != nil {
			t.Errorf("Get store for rule %s error: %s", ruleId, err)
			return
		}
		// Save for all checkpoints
		for i, cid := range checkpointIds {
			for j, opId := range opIds {
				err := store.SaveState(cid, opId, map[string]interface{}{
					"op": opId,
					"oi": j,
					"ci": i,
				})
				if err != nil {
					t.Errorf("Save state for rule %s op %s error: %s", ruleId, opId, err)
					return
				}
			}
			err := store.SaveCheckpoint(cid)
			if err != nil {
				t.Errorf("Save checkpoint %d for rule %s error: %s", cid, ruleId, err)
				return
			}
		}
		// compare checkpoints
		if !reflect.DeepEqual(checkpointIds, store.checkpoints) {
			t.Errorf("%d.Save checkpoint\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, checkpointIds, store.checkpoints)
		}
		// compare contents
		result := mapStoreToMap(store.mapStore)
		if !reflect.DeepEqual(r, result) {
			t.Errorf("%d.Save checkpoint\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, r, result)
		}
		// Save additional state but not serialized in checkpoint
		err = store.SaveState(10000, opIds[1], map[string]interface{}{
			"op": opIds[1],
			"oi": 1,
			"ci": 10000,
		})
		if err != nil {
			t.Errorf("Save state for rule %s op %s error: %s", ruleId, opIds[1], err)
			return
		}
		err = store.SaveState(10000, opIds[2], map[string]interface{}{
			"op": opIds[2],
			"oi": 2,
			"ci": 10000,
		})
		if err != nil {
			t.Errorf("Save state for rule %s op %s error: %s", ruleId, opIds[2], err)
			return
		}
		// compare checkpoints
		if !reflect.DeepEqual(checkpointIds, store.checkpoints) {
			t.Errorf("%d.Save state\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, checkpointIds, store.checkpoints)
		}
		// compare contents
		result = mapStoreToMap(store.mapStore)
		if !reflect.DeepEqual(rm, result) {
			t.Errorf("%d.Save state\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, r, result)
		}
		// simulate restore
		store = nil
		store, err = getKVStore(ruleId)
		if err != nil {
			t.Errorf("Restore store for rule %s error: %s", ruleId, err)
			return
		}
		// compare checkpoints
		if !reflect.DeepEqual(checkpointIds[2:], store.checkpoints) {
			t.Errorf("%d.Restore checkpoint\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, checkpointIds, store.checkpoints)
			return
		}
		// compare contents
		result = mapStoreToMap(store.mapStore)
		last := map[string]interface{}{
			"3": r["3"],
		}
		if !reflect.DeepEqual(last, result) {
			t.Errorf("%d.Restore checkpoint\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, last, result)
			return
		}
		ns, err := store.GetOpState(opIds[1])
		if err != nil {
			t.Errorf("Get op %s state for rule %s error: %s", opIds[1], ruleId, err)
			return
		}
		sm := r[fmt.Sprintf("%v", checkpointIds[len(checkpointIds)-1])].(map[string]interface{})[opIds[1]]
		nsm := cast.SyncMapToMap(ns)
		if !reflect.DeepEqual(sm, nsm) {
			t.Errorf("%d.Restore op state\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, sm, nsm)
			return
		}
	}()
}

func mapStoreToMap(sm *sync.Map) map[string]interface{} {
	m := make(map[string]interface{})
	sm.Range(func(k interface{}, v interface{}) bool {
		switch t := v.(type) {
		case *sync.Map:
			m[fmt.Sprintf("%v", k)] = mapStoreToMap(t)
		default:
			m[fmt.Sprintf("%v", k)] = t
		}
		return true
	})
	return m
}

func cleanStateData() {
	dbDir, err := conf.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}
	c := path.Join(dbDir)
	err = os.RemoveAll(c)
	if err != nil {
		conf.Log.Error(err)
	}
}
