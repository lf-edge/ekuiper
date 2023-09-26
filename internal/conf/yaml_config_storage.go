// Copyright 2023 EMQ Technologies Co., Ltd.
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

package conf

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

const (
	cfgFileStorage    = "file"
	cfgStoreKVStorage = "kv"
)

type cfgKVStorage interface {
	Set(string, map[string]interface{}) error
	Delete(string) error
	GetByPrefix(string) (map[string]map[string]interface{}, error)
}

type kvMemory struct {
	store map[string]map[string]interface{}
}

func (m *kvMemory) Set(key string, v map[string]interface{}) error {
	m.store[key] = v
	return nil
}

func (m *kvMemory) Delete(key string) error {
	delete(m.store, key)
	return nil
}

func (m *kvMemory) GetByPrefix(prefix string) (map[string]map[string]interface{}, error) {
	rm := make(map[string]map[string]interface{})
	for key, value := range m.store {
		if strings.HasPrefix(key, prefix) {
			rm[key] = value
		}
	}
	return rm, nil
}

var (
	mockMemoryKVStore *kvMemory
	sqliteKVStore     *sqlKVStore
)

func getKVStorage() (cfgKVStorage, error) {
	if IsTesting {
		if mockMemoryKVStore == nil {
			mockMemoryKVStore = &kvMemory{}
			mockMemoryKVStore.store = make(map[string]map[string]interface{})
		}
		return mockMemoryKVStore, nil
	}
	switch Config.Basic.CfgStorageType {
	case cfgStoreKVStorage:
		if sqliteKVStore == nil {
			sqliteKVStorage, err := NewSqliteKVStore("confKVStorage")
			if err != nil {
				return nil, err
			}
			sqliteKVStore = sqliteKVStorage
		}
		return sqliteKVStore, nil
	}
	return nil, fmt.Errorf("unknown cfg kv storage type: %v", Config.Basic.CfgStorageType)
}

func saveCfgKeyToKV(key string, cfg map[string]interface{}) error {
	kvStorage, err := getKVStorage()
	if err != nil {
		return err
	}
	return kvStorage.Set(key, cfg)
}

func delCfgKeyInStorage(key string) error {
	kvStorage, err := getKVStorage()
	if err != nil {
		return err
	}
	return kvStorage.Delete(key)
}

func getCfgKeyFromStorageByPrefix(prefix string) (map[string]map[string]interface{}, error) {
	kvStorage, err := getKVStorage()
	if err != nil {
		return nil, err
	}
	val, err := kvStorage.GetByPrefix(prefix)
	if err != nil {
		return nil, err
	}
	v := make(map[string]map[string]interface{})
	for key, value := range val {
		ss := strings.Split(key, ".")
		v[ss[2]] = value
	}
	return v, nil
}

func buildKey(confType string, pluginName string, confKey string) string {
	bs := bytes.NewBufferString(confType)
	if len(pluginName) < 1 {
		return bs.String()
	}
	bs.WriteString(".")
	bs.WriteString(pluginName)
	if len(confKey) < 1 {
		return bs.String()
	}
	bs.WriteString(".")
	bs.WriteString(confKey)
	return bs.String()
}

type sqlKVStore struct {
	kv kv.KeyValue
}

func NewSqliteKVStore(table string) (*sqlKVStore, error) {
	s := &sqlKVStore{}
	kv, err := store.GetKV(table)
	if err != nil {
		return nil, err
	}
	s.kv = kv
	return s, nil
}

func (s *sqlKVStore) Set(k string, v map[string]interface{}) error {
	return s.kv.Set(k, v)
}

func (s *sqlKVStore) Delete(k string) error {
	return s.kv.Delete(k)
}

func (s *sqlKVStore) GetByPrefix(prefix string) (map[string]map[string]interface{}, error) {
	keys, err := s.kv.Keys()
	if err != nil {
		return nil, err
	}
	r := make(map[string]map[string]interface{})
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			d := map[string]interface{}{}
			find, err := s.kv.Get(key, &d)
			if err != nil {
				return nil, err
			}
			if find {
				r[key] = d
			}
		}
	}
	return r, nil
}
