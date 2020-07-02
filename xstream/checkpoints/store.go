package checkpoints

import (
	"encoding/gob"
	"github.com/emqx/kuiper/common"
	"sync"
)

func init() {
	gob.Register(map[string]interface{}{})
}

//The manager for checkpoint storage. Right now, only support to store in badgerDB
type Store interface {
	SaveState(checkpointId int64, opId string, state map[string]interface{}) error
	RestoreState(opId string) map[string]interface{} //Get the state of an op, should only be used in initialization
	SaveCheckpoint(checkpointId int64) error         //Save the whole checkpoint state into storage like badger
}

type KVStore struct {
	db          common.KeyValue
	mapStore    *sync.Map
	checkpoints []int64
	max         int
}

//Store in path ./data/checkpoint/$ruleId
//Store 2 things:
//A queue for completed checkpoint id
//A map with key of checkpoint id and value of snapshot(gob serialized)
//The snapshot is a map also with key of opId and value of map
//Assume each operator only has one instance
func GetKVStore(ruleId string) *KVStore {
	db := common.GetSimpleKVStore("checkpoint/" + ruleId)
	s := &KVStore{db: db, max: 3}
	s.mapStore = &sync.Map{}
	return s
}

func (s *KVStore) SaveState(checkpointId int64, opId string, state map[string]interface{}) error {
	return nil
}

func (s *KVStore) RestoreState(opId string) map[string]interface{} {
	return nil
}

func (s *KVStore) SaveCheckpoint(checkpointId int64) error {
	return nil
}
