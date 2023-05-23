// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package store

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/internal/pkg/store/sql"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

type StoreCreator func(conf definition.Config, name string) (definition.StoreBuilder, definition.TsBuilder, error)

var (
	storeBuilders = map[string]StoreCreator{
		"sqlite": sql.BuildStores,
	}
	globalStores   *stores = nil
	cacheStores    *stores = nil
	extStateStores *stores = nil
)

type stores struct {
	kv        map[string]kv.KeyValue
	ts        map[string]kv.Tskv
	mu        sync.Mutex
	kvBuilder definition.StoreBuilder
	tsBuilder definition.TsBuilder
}

func newStores(c definition.Config, name string) (*stores, error) {
	databaseType := c.Type
	if builder, ok := storeBuilders[databaseType]; ok {
		kvBuilder, tsBuilder, err := builder(c, name)
		if err != nil {
			return nil, err
		} else {
			return &stores{
				kv:        make(map[string]kv.KeyValue),
				ts:        make(map[string]kv.Tskv),
				mu:        sync.Mutex{},
				kvBuilder: kvBuilder,
				tsBuilder: tsBuilder,
			}, nil
		}
	} else {
		return nil, fmt.Errorf("unknown database type: %s", databaseType)
	}
}

func newExtStateStores(c definition.Config, name string) (*stores, error) {
	databaseType := c.ExtStateType
	if builder, ok := storeBuilders[databaseType]; ok {
		kvBuilder, tsBuilder, err := builder(c, name)
		if err != nil {
			return nil, err
		} else {
			return &stores{
				kv:        make(map[string]kv.KeyValue),
				ts:        make(map[string]kv.Tskv),
				mu:        sync.Mutex{},
				kvBuilder: kvBuilder,
				tsBuilder: tsBuilder,
			}, nil
		}
	} else {
		return nil, fmt.Errorf("unknown extStateStore type: %s", databaseType)
	}
}

func (s *stores) GetKV(table string) (kv.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ks, contains := s.kv[table]; contains {
		return ks, nil
	}
	ks, err := s.kvBuilder.CreateStore(table)
	if err != nil {
		return nil, err
	}
	s.kv[table] = ks
	return ks, nil
}

func (s *stores) DropKV(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ks, contains := s.kv[table]; contains {
		_ = ks.Drop()
		delete(s.ts, table)
	}
}

func (s *stores) DropRefKVs(tablePrefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for table, ks := range s.kv {
		if strings.HasPrefix(table, tablePrefix) {
			_ = ks.Drop()
			delete(s.kv, table)
		}
	}
}

func (s *stores) GetTS(table string) (kv.Tskv, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tts, contains := s.ts[table]; contains {
		return tts, nil
	}
	tts, err := s.tsBuilder.CreateTs(table)
	if err != nil {
		return nil, err
	}
	s.ts[table] = tts
	return tts, nil
}

func (s *stores) DropTS(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tts, contains := s.ts[table]; contains {
		_ = tts.Drop()
		delete(s.ts, table)
	}
}

func GetKV(table string) (kv.KeyValue, error) {
	if globalStores == nil {
		return nil, fmt.Errorf("global stores are not initialized")
	}
	return globalStores.GetKV(table)
}

func GetTS(table string) (kv.Tskv, error) {
	if globalStores == nil {
		return nil, fmt.Errorf("global stores are not initialized")
	}
	return globalStores.GetTS(table)
}

func DropTS(table string) error {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized")
	}
	globalStores.DropTS(table)
	return nil
}

func DropKV(table string) error {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized")
	}
	globalStores.DropKV(table)
	return nil
}

func GetCacheKV(table string) (kv.KeyValue, error) {
	if cacheStores == nil {
		return nil, fmt.Errorf("cache stores are not initialized")
	}
	return cacheStores.GetKV(table)
}

func DropCacheKV(table string) error {
	if cacheStores == nil {
		return fmt.Errorf("cache stores are not initialized")
	}
	cacheStores.DropKV(table)
	return nil
}

func DropCacheKVForRule(rule string) error {
	if cacheStores == nil {
		return fmt.Errorf("cache stores are not initialized")
	}
	cacheStores.DropRefKVs(path.Join("sink", rule))
	return nil
}

func GetExtStateKV(table string) (kv.KeyValue, error) {
	if extStateStores == nil {
		return nil, fmt.Errorf("extState stores are not initialized")
	}
	return extStateStores.GetKV(table)
}
