package states

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/checkpoints"
	"path"
	"sync"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(checkpoints.BufferOrEvent{})
}

//The manager for checkpoint storage.
/**
*** mapStore keys
***   { "checkpoint1", "checkpoint2" ... "checkpointn" : The complete or incomplete snapshot
***   "op1", "op2" ... "opn": the current state for all ops
 */
type KVStore struct {
	db          common.KeyValue
	mapStore    *sync.Map //The current root store of a rule
	checkpoints []int64
	max         int
}

//Store in path ./data/checkpoint/$ruleId
//Store 2 things:
//"checkpoints":A queue for completed checkpoint id
//"$checkpointId":A map with key of checkpoint id and value of snapshot(gob serialized)
//The snapshot is a map also with key of opId and value of map
//Assume each operator only has one instance
func getKVStore(ruleId string) (*KVStore, error) {
	dr, _ := common.GetDataLoc()
	db := common.GetSimpleKVStore(path.Join(dr, "checkpoints", ruleId))
	s := &KVStore{db: db, max: 3}
	//read data from badger db
	if err := s.restore(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *KVStore) restore() error {
	err := s.db.Open()
	if err != nil {
		return err
	}
	defer s.db.Close()
	if bytes, ok := s.db.Get(CheckpointListKey); ok {
		if cs, err := bytesToSlice(bytes.([]byte)); err != nil {
			return fmt.Errorf("invalid checkpoint data: %s", err)
		} else {
			s.checkpoints = cs
			if bytes, ok := s.db.Get(string(cs[len(cs)-1])); ok {
				if m, err := bytesToMap(bytes.([]byte)); err != nil {
					return fmt.Errorf("invalid last checkpoint data: %s", err)
				} else {
					s.mapStore = m
					return nil
				}
			}
		}

	}
	s.mapStore = &sync.Map{}
	return nil
}

func (s *KVStore) SaveState(checkpointId int64, opId string, state map[string]interface{}) error {
	logger := common.Log
	logger.Debugf("Save state for checkpoint %d, op %s, value %v", checkpointId, opId, state)
	var cstore *sync.Map
	if v, ok := s.mapStore.Load(checkpointId); !ok {
		cstore = &sync.Map{}
		s.mapStore.Store(checkpointId, cstore)
	} else {
		if cstore, ok = v.(*sync.Map); !ok {
			return fmt.Errorf("invalid KVStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		}
	}
	cstore.Store(opId, state)
	return nil
}

func (s *KVStore) SaveCheckpoint(checkpointId int64) error {
	if v, ok := s.mapStore.Load(checkpointId); !ok {
		return fmt.Errorf("store for checkpoint %d not found", checkpointId)
	} else {
		if m, ok := v.(*sync.Map); !ok {
			return fmt.Errorf("invalid KVStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		} else {
			err := s.db.Open()
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
			defer s.db.Close()
			b, err := mapToBytes(m)
			if err != nil {
				return fmt.Errorf("save checkpoint err, fail to encode states: %s", err)
			}
			err = s.db.Set(string(checkpointId), b)
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
			m.Delete(checkpointId)
			s.checkpoints = append(s.checkpoints, checkpointId)
			//TODO is the order promised?
			if len(s.checkpoints) > s.max {
				cp := s.checkpoints[0]
				s.checkpoints = s.checkpoints[1:]
				go func() {
					s.db.Delete(string(cp))
				}()
			}
			cs, ok := sliceToBytes(s.checkpoints)
			if !ok {
				return fmt.Errorf("save checkpoint err: fail to encode checkpoint counts")
			}
			err = s.db.Set(CheckpointListKey, cs)
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
		}
	}
	return nil
}

//Only run in the initialization
func (s *KVStore) GetOpState(opId string) (*sync.Map, error) {
	if sm, ok := s.mapStore.Load(opId); ok {
		switch m := sm.(type) {
		case *sync.Map:
			return m, nil
		case map[string]interface{}:
			return common.MapToSyncMap(m), nil
		default:
			return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not *sync.Map", sm, opId)
		}
	} else {
		return &sync.Map{}, nil
	}
}

func mapToBytes(sm *sync.Map) ([]byte, error) {
	m := common.SyncMapToMap(sm)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func bytesToMap(input []byte) (*sync.Map, error) {
	var result map[string]interface{}
	buf := bytes.NewBuffer(input)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}
	return common.MapToSyncMap(result), nil
}

func sliceToBytes(s []int64) ([]byte, bool) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

func bytesToSlice(input []byte) ([]int64, error) {
	result := make([]int64, 3)
	buf := bytes.NewBuffer(input)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
