// Copyright 2021-2026 EMQ Technologies Co., Ltd.
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
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
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
		dataDir, err := conf.GetDataLoc()
		if err != nil {
			t.Error(err)
		}
		cleanStateData()
		err = store.SetupDefault(dataDir)
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
				err := store.SaveFrozenState(cid, opId, mustEncodeState(t, map[string]interface{}{
					"op": opId,
					"oi": j,
					"ci": i,
				}))
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
		err = store.SaveFrozenState(10000, opIds[1], mustEncodeState(t, map[string]interface{}{
			"op": opIds[1],
			"oi": 1,
			"ci": 10000,
		}))
		if err != nil {
			t.Errorf("Save state for rule %s op %s error: %s", ruleId, opIds[1], err)
			return
		}
		err = store.SaveFrozenState(10000, opIds[2], mustEncodeState(t, map[string]interface{}{
			"op": opIds[2],
			"oi": 2,
			"ci": 10000,
		}))
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

func TestRestoreLegacyCheckpointAndWriteVersion2(t *testing.T) {
	const (
		legacyID     = int64(100)
		checkpointID = int64(101)
		opID         = "op1"
	)
	legacyState := map[string]interface{}{"count": 42, "name": "legacy"}
	db := &memoryTSKV{}
	inserted, err := db.Set(legacyID, map[string]interface{}{opID: legacyState})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("legacy checkpoint was not inserted")
	}

	restored := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
	if err := restored.restore(); err != nil {
		t.Fatal(err)
	}
	gotState, err := restored.GetOpState(opID)
	if err != nil {
		t.Fatal(err)
	}
	if got := cast.SyncMapToMap(gotState); !reflect.DeepEqual(legacyState, got) {
		t.Fatalf("legacy state mismatch: want %#v, got %#v", legacyState, got)
	}

	nextState := map[string]interface{}{"count": 43, "name": "version2"}
	if err := restored.SaveFrozenState(checkpointID, opID, mustEncodeState(t, nextState)); err != nil {
		t.Fatal(err)
	}
	if err := restored.SaveCheckpoint(checkpointID); err != nil {
		t.Fatal(err)
	}
	var envelope checkpointEnvelope
	key, err := restored.db.Last(&envelope)
	if err != nil {
		t.Fatal(err)
	}
	if key != checkpointID {
		t.Fatalf("checkpoint ID mismatch: want %d, got %d", checkpointID, key)
	}
	if envelope.Format != checkpointFormat || envelope.Version != checkpointFormatVersion {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
	frozen, ok := envelope.Operators[opID]
	if !ok {
		t.Fatalf("operator %s not found in envelope", opID)
	}
	decoded, err := checkpoint.DecodeState(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nextState, decoded) {
		t.Fatalf("version 2 state mismatch: want %#v, got %#v", nextState, decoded)
	}
}

func TestLegacyRestoredOperatorStateIsolatedAcrossGetOpState(t *testing.T) {
	const (
		checkpointID = int64(102)
		opID         = "op1"
	)
	db := &memoryTSKV{}
	legacyState := map[string]interface{}{
		"nested": map[string]interface{}{"count": 1},
	}
	inserted, err := db.Set(checkpointID, map[string]interface{}{opID: legacyState})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("legacy checkpoint was not inserted")
	}

	restored := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
	if err := restored.restore(); err != nil {
		t.Fatal(err)
	}
	first, err := restored.GetOpState(opID)
	if err != nil {
		t.Fatal(err)
	}
	firstNested, _ := first.Load("nested")
	firstNested.(map[string]interface{})["count"] = 2

	second, err := restored.GetOpState(opID)
	if err != nil {
		t.Fatal(err)
	}
	secondNested, _ := second.Load("nested")
	if got := secondNested.(map[string]interface{})["count"]; got != 1 {
		t.Fatalf("legacy restore shared nested live state: got %v", got)
	}
}

type memoryTSKV struct {
	last       int64
	data       map[int64][]byte
	setStarted chan struct{}
	allowSet   chan struct{}
}

func (m *memoryTSKV) Set(key int64, value interface{}) (bool, error) {
	if m.setStarted != nil {
		m.setStarted <- struct{}{}
		<-m.allowSet
	}
	if key <= m.last {
		return false, nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return false, err
	}
	if m.data == nil {
		m.data = make(map[int64][]byte)
	}
	m.data[key] = buf.Bytes()
	m.last = key
	return true, nil
}

func (m *memoryTSKV) Get(key int64, value interface{}) (bool, error) {
	data, ok := m.data[key]
	if !ok {
		return false, nil
	}
	return true, gob.NewDecoder(bytes.NewReader(data)).Decode(value)
}

func (m *memoryTSKV) Last(value interface{}) (int64, error) {
	if m.last == 0 {
		return 0, nil
	}
	_, err := m.Get(m.last, value)
	return m.last, err
}

func (m *memoryTSKV) Delete(key int64) error {
	delete(m.data, key)
	return nil
}

func (m *memoryTSKV) DeleteBefore(key int64) error {
	for candidate := range m.data {
		if candidate < key {
			delete(m.data, candidate)
		}
	}
	return nil
}

func (m *memoryTSKV) Close() error {
	return nil
}

func (m *memoryTSKV) Drop() error {
	m.data = nil
	m.last = 0
	return nil
}

func TestSaveFrozenStateConcurrentFirstWriters(t *testing.T) {
	s := &KVStore{mapStore: &sync.Map{}}
	const operatorCount = 100
	errCh := make(chan error, operatorCount)
	var wg sync.WaitGroup
	for i := 0; i < operatorCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			opID := fmt.Sprintf("op-%d", index)
			errCh <- s.SaveFrozenState(1, opID, []byte{byte(index)})
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	raw, ok := s.mapStore.Load(int64(1))
	if !ok {
		t.Fatal("checkpoint store was not created")
	}
	cstore := raw.(*sync.Map)
	count := 0
	cstore.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != operatorCount {
		t.Fatalf("operator count mismatch: want %d, got %d", operatorCount, count)
	}
	if err := s.SaveFrozenState(1, "op-0", []byte("replacement")); err == nil {
		t.Fatal("duplicate operator state must be rejected")
	}
	original, ok := cstore.Load("op-0")
	if !ok || !bytes.Equal(original.([]byte), []byte{0}) {
		t.Fatal("duplicate operator state changed the original snapshot")
	}
}

func TestGetOpStateRejectsCorruptFrozenState(t *testing.T) {
	cstore := &sync.Map{}
	cstore.Store("op1", []byte("not-gob"))
	s := &KVStore{
		mapStore:    &sync.Map{},
		checkpoints: []int64{1},
	}
	s.mapStore.Store(int64(1), cstore)
	if _, err := s.GetOpState("op1"); err == nil {
		t.Fatal("corrupt frozen state must fail")
	}
}

func TestRestoreRejectsCorruptVersion2OperatorState(t *testing.T) {
	const (
		checkpointID = int64(7)
		opID         = "op1"
	)
	db := &memoryTSKV{}
	inserted, err := db.Set(checkpointID, checkpointEnvelope{
		Format:  checkpointFormat,
		Version: checkpointFormatVersion,
		Operators: map[string][]byte{
			opID: []byte("not-gob"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("test envelope was not inserted")
	}

	s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
	err = s.restore()
	if err == nil {
		t.Fatal("corrupt operator state must fail restore")
	}
	if !strings.Contains(err.Error(), "checkpoint 7") || !strings.Contains(err.Error(), "operator op1") {
		t.Fatalf("restore error lacks checkpoint/operator context: %v", err)
	}
	if len(s.checkpoints) != 0 {
		t.Fatalf("failed restore published checkpoints: %v", s.checkpoints)
	}
	if _, ok := s.mapStore.Load(checkpointID); ok {
		t.Fatal("failed restore published corrupt checkpoint state")
	}
}

func TestRestoredOperatorStateIsolatedAcrossGetOpState(t *testing.T) {
	const (
		checkpointID = int64(8)
		opID         = "op1"
	)
	original := map[string]interface{}{
		"nested": map[string]interface{}{
			"count": 1,
		},
	}
	db := &memoryTSKV{}
	inserted, err := db.Set(checkpointID, checkpointEnvelope{
		Format:  checkpointFormat,
		Version: checkpointFormatVersion,
		Operators: map[string][]byte{
			opID: mustEncodeState(t, original),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("test envelope was not inserted")
	}

	s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
	if err := s.restore(); err != nil {
		t.Fatal(err)
	}

	first, err := s.GetOpState(opID)
	if err != nil {
		t.Fatal(err)
	}
	firstNested, ok := first.Load("nested")
	if !ok {
		t.Fatal("first restored state lacks nested value")
	}
	firstNested.(map[string]interface{})["count"] = 2

	second, err := s.GetOpState(opID)
	if err != nil {
		t.Fatal(err)
	}
	secondNested, ok := second.Load("nested")
	if !ok {
		t.Fatal("second restored state lacks nested value")
	}
	if got := secondNested.(map[string]interface{})["count"]; got != 1 {
		t.Fatalf("second restored state observed first context mutation: got %v", got)
	}
	secondNested.(map[string]interface{})["count"] = 3

	third, err := s.GetOpState(opID)
	if err != nil {
		t.Fatal(err)
	}
	thirdNested, ok := third.Load("nested")
	if !ok {
		t.Fatal("third restored state lacks nested value")
	}
	if got := thirdNested.(map[string]interface{})["count"]; got != 1 {
		t.Fatalf("third restored state observed earlier context mutation: got %v", got)
	}
}

func TestRestoreRejectsInvalidVersion2Envelope(t *testing.T) {
	tests := []struct {
		name     string
		envelope checkpointEnvelope
	}{
		{
			name: "unknown version",
			envelope: checkpointEnvelope{
				Format:    checkpointFormat,
				Version:   checkpointFormatVersion + 1,
				Operators: map[string][]byte{},
			},
		},
		{
			name: "nil operators",
			envelope: checkpointEnvelope{
				Format:  checkpointFormat,
				Version: checkpointFormatVersion,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := &memoryTSKV{}
			inserted, err := db.Set(1, test.envelope)
			if err != nil {
				t.Fatal(err)
			}
			if !inserted {
				t.Fatal("test envelope was not inserted")
			}
			s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
			if err := s.restore(); err == nil {
				t.Fatal("invalid version 2 envelope must fail")
			}
		})
	}
}

func TestSaveCheckpointRejectsDuplicateID(t *testing.T) {
	db := &memoryTSKV{}
	s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
	state := mustEncodeState(t, map[string]interface{}{"count": 1})
	if err := s.SaveFrozenState(1, "op", state); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(1); err != nil {
		t.Fatal(err)
	}

	s.mapStore.Delete(int64(1))
	if err := s.SaveFrozenState(1, "op", state); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(1); err == nil {
		t.Fatal("duplicate checkpoint ID must fail")
	}
}

func TestDiscardFrozenStateRejectsLateSnapshot(t *testing.T) {
	s := &KVStore{db: &memoryTSKV{}, max: 3, mapStore: &sync.Map{}}
	if err := s.SaveFrozenState(1, "op1", []byte("first")); err != nil {
		t.Fatal(err)
	}
	s.DiscardFrozenState(1)
	if _, ok := s.mapStore.Load(int64(1)); ok {
		t.Fatal("discarded checkpoint remained in memory")
	}
	if err := s.SaveFrozenState(1, "op2", []byte("late")); err == nil {
		t.Fatal("late snapshot for discarded checkpoint must fail")
	}
	if _, ok := s.mapStore.Load(int64(1)); ok {
		t.Fatal("late snapshot recreated a discarded checkpoint")
	}
	if err := s.SaveCheckpoint(1); err == nil {
		t.Fatal("discarded checkpoint must not be persisted")
	}
	if s.discardedThrough != 1 {
		t.Fatalf("discard watermark mismatch: got %d", s.discardedThrough)
	}
	if err := s.SaveFrozenState(0, "older", []byte("older")); err == nil {
		t.Fatal("discard watermark must reject older checkpoints")
	}
	if err := s.SaveFrozenState(2, "newer", []byte("newer")); err != nil {
		t.Fatalf("discard watermark rejected newer checkpoint: %v", err)
	}
}

func TestSaveCheckpointAndDiscardAreSerialized(t *testing.T) {
	db := &memoryTSKV{
		setStarted: make(chan struct{}),
		allowSet:   make(chan struct{}),
	}
	s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}}
	if err := s.SaveFrozenState(1, "op", mustEncodeState(t, map[string]interface{}{"count": 1})); err != nil {
		t.Fatal(err)
	}

	saveDone := make(chan error, 1)
	go func() {
		saveDone <- s.SaveCheckpoint(1)
	}()
	<-db.setStarted

	discardStarted := make(chan struct{})
	discardDone := make(chan struct{})
	go func() {
		close(discardStarted)
		s.DiscardFrozenState(1)
		close(discardDone)
	}()
	<-discardStarted
	select {
	case <-discardDone:
		t.Fatal("discard completed while SaveCheckpoint held the lifecycle lock")
	default:
	}

	close(db.allowSet)
	if err := <-saveDone; err != nil {
		t.Fatal(err)
	}
	<-discardDone
	if s.discardedThrough != 0 {
		t.Fatal("discard invalidated a checkpoint that completed first")
	}
	if _, ok := s.mapStore.Load(int64(1)); !ok {
		t.Fatal("discard removed a checkpoint that completed first")
	}
}

func mapStoreToMap(sm *sync.Map) map[string]interface{} {
	m := make(map[string]interface{})
	sm.Range(func(k interface{}, v interface{}) bool {
		switch t := v.(type) {
		case *sync.Map:
			m[fmt.Sprintf("%v", k)] = mapStoreToMap(t)
		case *restoredOperatorState:
			state, err := checkpoint.DecodeState(t.frozen)
			if err != nil {
				panic(err)
			}
			m[fmt.Sprintf("%v", k)] = state
		case []byte:
			state, err := checkpoint.DecodeState(t)
			if err != nil {
				panic(err)
			}
			m[fmt.Sprintf("%v", k)] = state
		default:
			m[fmt.Sprintf("%v", k)] = t
		}
		return true
	})
	return m
}

func mustEncodeState(t *testing.T, state map[string]interface{}) []byte {
	t.Helper()
	result, err := checkpoint.EncodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	return result
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
