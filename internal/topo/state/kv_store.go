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
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	ts "github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	ts2 "github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/store"
)

const (
	checkpointFormat        = "ekuiper-checkpoint"
	checkpointFormatVersion = uint16(2)
)

type checkpointEnvelope struct {
	Format    string
	Version   uint16
	Operators map[string][]byte
}

// restoredOperatorState retains the persisted snapshot while transferring the
// eagerly decoded object graph to at most one live operator context.
type restoredOperatorState struct {
	mu      sync.Mutex
	frozen  []byte
	decoded *sync.Map
}

func newRestoredOperatorState(frozen []byte) (*restoredOperatorState, error) {
	decoded, err := checkpoint.DecodeState(frozen)
	if err != nil {
		return nil, err
	}
	return newRestoredOperatorStateFromDecoded(frozen, decoded), nil
}

func newRestoredOperatorStateFromDecoded(frozen []byte, decoded map[string]interface{}) *restoredOperatorState {
	return &restoredOperatorState{
		frozen:  frozen,
		decoded: cast.MapToSyncMap(decoded),
	}
}

func (s *restoredOperatorState) take() (*sync.Map, error) {
	s.mu.Lock()
	if s.decoded != nil {
		decoded := s.decoded
		s.decoded = nil
		s.mu.Unlock()
		return decoded, nil
	}
	frozen := s.frozen
	s.mu.Unlock()

	decoded, err := checkpoint.DecodeState(frozen)
	if err != nil {
		return nil, err
	}
	return cast.MapToSyncMap(decoded), nil
}

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(checkpoint.BufferOrEvent{})
	gob.Register(&store.IndexFieldStore{})
}

// KVStore The manager for checkpoint storage.
//
// mapStore keys
//
//	{ "checkpoint1", "checkpoint2" ... "checkpointn" : The complete or incomplete snapshot
type KVStore struct {
	db          ts2.Tskv
	mapStore    *sync.Map // The current root store of a rule
	lifecycleMu sync.RWMutex
	// discardedThrough is a monotonic tombstone. A canceled checkpoint and
	// every older checkpoint remain invalid without retaining one map entry
	// per cancellation.
	discardedThrough int64
	checkpoints      []int64
	max              int
	ruleId           string
}

// Store in path ./data/checkpoint/$ruleId
// Store 2 things:
// "checkpoints":A queue for completed checkpoint id
// "$checkpointId":A map with key of checkpoint id and value of snapshot(gob serialized)
// Assume each operator only has one instance
func getKVStore(ruleId string) (*KVStore, error) {
	db, err := ts.GetTS(ruleId)
	if err != nil {
		return nil, err
	}
	s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}, ruleId: ruleId}
	// read data from badger db
	if err := s.restore(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *KVStore) restore() error {
	var envelope checkpointEnvelope
	k, envelopeErr := s.db.Last(&envelope)
	if envelopeErr == nil && k == 0 {
		return nil
	}
	if envelopeErr == nil && envelope.Format == checkpointFormat {
		if envelope.Version != checkpointFormatVersion {
			return fmt.Errorf("unsupported checkpoint format version %d", envelope.Version)
		}
		if envelope.Operators == nil {
			return fmt.Errorf("invalid checkpoint %d: operators must not be nil", k)
		}
		cstore := &sync.Map{}
		for opID, state := range envelope.Operators {
			restored, err := newRestoredOperatorState(state)
			if err != nil {
				return fmt.Errorf("restore checkpoint %d state for operator %s: %w", k, opID, err)
			}
			cstore.Store(opID, restored)
		}
		s.checkpoints = []int64{k}
		s.mapStore.Store(k, cstore)
		return nil
	}

	var legacy map[string]interface{}
	legacyKey, legacyErr := s.db.Last(&legacy)
	if legacyErr != nil {
		if envelopeErr != nil {
			return fmt.Errorf("decode checkpoint as version %d: %v; decode as legacy: %w", checkpointFormatVersion, envelopeErr, legacyErr)
		}
		return legacyErr
	}
	if legacyKey > 0 {
		cstore := &sync.Map{}
		for opID, value := range legacy {
			operatorState, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("restore legacy checkpoint %d state for operator %s: invalid type %T", legacyKey, opID, value)
			}
			frozen, err := checkpoint.EncodeState(operatorState)
			if err != nil {
				return fmt.Errorf("restore legacy checkpoint %d state for operator %s: %w", legacyKey, opID, err)
			}
			cstore.Store(opID, newRestoredOperatorStateFromDecoded(frozen, operatorState))
		}
		s.checkpoints = []int64{legacyKey}
		s.mapStore.Store(legacyKey, cstore)
	}
	return nil
}

func (s *KVStore) SaveState(checkpointId int64, opId string, state map[string]interface{}) error {
	logger := conf.Log
	logger.Debugf("Save state for checkpoint %d, op %s, value %v", checkpointId, opId, state)
	frozen, err := checkpoint.EncodeState(state)
	if err != nil {
		return err
	}
	return s.SaveFrozenState(checkpointId, opId, frozen)
}

func (s *KVStore) SaveFrozenState(checkpointID int64, opID string, state []byte) error {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if checkpointID <= s.discardedThrough {
		return fmt.Errorf("checkpoint %d was discarded", checkpointID)
	}
	cstore, err := s.loadOrCreateCheckpoint(checkpointID)
	if err != nil {
		return err
	}
	if _, loaded := cstore.LoadOrStore(opID, state); loaded {
		return fmt.Errorf("state for checkpoint %d operator %s already exists", checkpointID, opID)
	}
	return nil
}

func (s *KVStore) DiscardFrozenState(checkpointID int64) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	for _, completedID := range s.checkpoints {
		if completedID == checkpointID {
			return
		}
	}
	if checkpointID > s.discardedThrough {
		s.discardedThrough = checkpointID
	}
	s.mapStore.Delete(checkpointID)
}

func (s *KVStore) loadOrCreateCheckpoint(checkpointID int64) (*sync.Map, error) {
	candidate := &sync.Map{}
	actual, _ := s.mapStore.LoadOrStore(checkpointID, candidate)
	cstore, ok := actual.(*sync.Map)
	if !ok {
		return nil, fmt.Errorf("invalid KVStore for checkpointId %d with value %v: should be *sync.Map type", checkpointID, actual)
	}
	return cstore, nil
}

func (s *KVStore) SaveCheckpoint(checkpointId int64) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if checkpointId <= s.discardedThrough {
		return fmt.Errorf("checkpoint %d was discarded", checkpointId)
	}
	if v, ok := s.mapStore.Load(checkpointId); !ok {
		return fmt.Errorf("store for checkpoint %d not found", checkpointId)
	} else {
		if m, ok := v.(*sync.Map); !ok {
			return fmt.Errorf("invalid KVStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		} else {
			operators := make(map[string][]byte)
			var rangeErr error
			m.Range(func(key, value interface{}) bool {
				opID, keyOK := key.(string)
				state, valueOK := value.([]byte)
				if !keyOK || !valueOK {
					rangeErr = fmt.Errorf("invalid frozen state for checkpoint %d: operator %v has type %T", checkpointId, key, value)
					return false
				}
				operators[opID] = state
				return true
			})
			if rangeErr != nil {
				return rangeErr
			}
			envelope := checkpointEnvelope{
				Format:    checkpointFormat,
				Version:   checkpointFormatVersion,
				Operators: operators,
			}
			inserted, err := s.db.Set(checkpointId, envelope)
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
			if !inserted {
				return fmt.Errorf("checkpoint %d was not inserted", checkpointId)
			}
			s.checkpoints = append(s.checkpoints, checkpointId)
			for len(s.checkpoints) > s.max {
				cp := s.checkpoints[0]
				s.checkpoints = s.checkpoints[1:]
				s.mapStore.Delete(cp)
			}
		}
	}
	return nil
}

// GetOpState Only run in the initialization
func (s *KVStore) GetOpState(opId string) (*sync.Map, error) {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if len(s.checkpoints) > 0 {
		if v, ok := s.mapStore.Load(s.checkpoints[len(s.checkpoints)-1]); ok {
			if cstore, ok := v.(*sync.Map); !ok {
				return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not *sync.Map", v, opId)
			} else {
				if sm, ok := cstore.Load(opId); ok {
					switch m := sm.(type) {
					case []byte:
						state, err := checkpoint.DecodeState(m)
						if err != nil {
							return nil, fmt.Errorf("restore state for operator %s: %w", opId, err)
						}
						return cast.MapToSyncMap(state), nil
					case *restoredOperatorState:
						state, err := m.take()
						if err != nil {
							return nil, fmt.Errorf("restore state for operator %s: %w", opId, err)
						}
						return state, nil
					case *sync.Map:
						return m, nil
					case map[string]interface{}:
						return cast.MapToSyncMap(m), nil
					default:
						return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not *sync.Map", sm, opId)
					}
				}
			}
		} else {
			return nil, fmt.Errorf("store for checkpoint %d not found", s.checkpoints[len(s.checkpoints)-1])
		}
	}
	return &sync.Map{}, nil
}

func (s *KVStore) Clean() error {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if len(s.checkpoints) == 0 {
		return nil
	}
	return s.db.DeleteBefore(s.checkpoints[0])
}
