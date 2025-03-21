// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

const (
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
	kvStore           *sqlKVStore
)

// GetYamlConfigAllKeys get all plugin keys about sources/sinks/connections
func GetYamlConfigAllKeys(typ string) (map[string]struct{}, error) {
	s, err := getKVStorage()
	if err != nil {
		return nil, err
	}
	data, err := s.GetByPrefix(typ)
	failpoint.Inject("getDataErr", func() {
		err = errors.New("getDataErr")
	})
	if err != nil {
		return nil, err
	}
	s1 := make(map[string]struct{})
	for key := range data {
		names := strings.Split(key, ".")
		if len(names) != 3 {
			continue
		}
		s1[names[1]] = struct{}{}
	}
	return s1, nil
}

func getKVStorage() (s cfgKVStorage, err error) {
	defer func() {
		failpoint.Inject("storageErr", func() {
			err = errors.New("storageErr")
		})
	}()
	if IsTesting {
		if mockMemoryKVStore == nil {
			mockMemoryKVStore = &kvMemory{}
			mockMemoryKVStore.store = make(map[string]map[string]interface{})
		}
		return mockMemoryKVStore, nil
	}
	if kvStore == nil {
		sqliteKVStorage, err := NewSqliteKVStore("confKVStorage")
		if err != nil {
			return nil, err
		}
		kvStore = sqliteKVStorage
	}
	return kvStore, nil
}

// SaveCfgKeyToKV ...
func SaveCfgKeyToKV(key string, cfg map[string]interface{}) error {
	return saveCfgKeyToKV(key, cfg)
}

func LoadCfgKeyKV(key string) (map[string]interface{}, error) {
	kvStorage, err := getKVStorage()
	if err != nil {
		return nil, err
	}
	mmap, err := kvStorage.GetByPrefix(key)
	if err != nil {
		return nil, err
	}
	Log.Infof("load conf key:%v ", key)
	return mmap[key], nil
}

func saveCfgKeyToKV(key string, cfg map[string]interface{}) error {
	kvStorage, err := getKVStorage()
	if err != nil {
		return err
	}
	Log.Infof("write conf key:%v ", key)
	return kvStorage.Set(key, cfg)
}

func delCfgKeyInStorage(key string) error {
	kvStorage, err := getKVStorage()
	if err != nil {
		return err
	}
	Log.Infof("del conf key:%v ", key)
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
		// skip data if not conf
		if len(ss) != 3 {
			continue
		}
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

// WriteCfgIntoKVStorage ...
func WriteCfgIntoKVStorage(typ string, plugin string, confKey string, confData map[string]interface{}) error {
	key := buildKey(typ, plugin, confKey)
	return saveCfgKeyToKV(key, confData)
}

// DropCfgKeyFromStorage ...
func DropCfgKeyFromStorage(typ string, plugin string, confKey string) error {
	key := buildKey(typ, plugin, confKey)
	return delCfgKeyInStorage(key)
}

// GetCfgFromKVStorage ...
func GetCfgFromKVStorage(typ string, plugin string, confKey string) (map[string]map[string]interface{}, error) {
	key := buildKey(typ, plugin, confKey)
	kvStorage, err := getKVStorage()
	if err != nil {
		return nil, err
	}
	return kvStorage.GetByPrefix(key)
}

// ClearKVStorage only used in unit test
func ClearKVStorage() error {
	kvStorage, err := getKVStorage()
	if err != nil {
		return err
	}
	km, err := kvStorage.GetByPrefix("")
	if err != nil {
		return err
	}
	for key := range km {
		kvStorage.Delete(key)
	}
	return nil
}

// GetAllConnConfigs return connections' plugin -> confKey -> props
func GetAllConnConfigs() (map[string]map[string]map[string]any, error) {
	allConfigs, err := GetCfgFromKVStorage("connections", "", "")
	if err != nil {
		return nil, err
	}
	got := make(map[string]map[string]map[string]any)
	for key, props := range allConfigs {
		_, plugin, confKey, err := splitKey(key)
		if err != nil {
			continue
		}
		pluginProps, ok := got[plugin]
		if !ok {
			pluginProps = make(map[string]map[string]any)
			got[plugin] = pluginProps
		}
		pluginProps[confKey] = props
	}
	return got, nil
}

func splitKey(key string) (string, string, string, error) {
	keys := strings.Split(key, ".")
	if len(keys) != 3 {
		return "", "", "", fmt.Errorf("invalid key: %s", key)
	}
	return keys[0], keys[1], keys[2], nil
}
