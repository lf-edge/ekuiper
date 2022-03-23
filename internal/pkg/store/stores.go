// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/internal/pkg/store/sql"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"sync"
)

type StoreCreator func(conf definition.Config) (definition.StoreBuilder, definition.TsBuilder, error)

var (
	storeBuilders = map[string]StoreCreator{
		"sqlite": sql.BuildStores,
	}
	globalStores *stores = nil
)

type stores struct {
	kv        map[string]kv.KeyValue
	ts        map[string]kv.Tskv
	mu        sync.Mutex
	kvBuilder definition.StoreBuilder
	tsBuilder definition.TsBuilder
}

func newStores(c definition.Config) (*stores, error) {
	databaseType := c.Type
	if builder, ok := storeBuilders[databaseType]; ok {
		kvBuilder, tsBuilder, err := builder(c)
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

func (s *stores) GetKV(table string) (error, kv.KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ks, contains := s.kv[table]; contains {
		return nil, ks
	}
	ks, err := s.kvBuilder.CreateStore(table)
	if err != nil {
		return err, nil
	}
	s.kv[table] = ks
	return nil, ks
}

func (s *stores) DropKV(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ks, contains := s.kv[table]; contains {
		_ = ks.Drop()
		delete(s.ts, table)
	}
}

func (s *stores) GetTS(table string) (error, kv.Tskv) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tts, contains := s.ts[table]; contains {
		return nil, tts
	}
	err, tts := s.tsBuilder.CreateTs(table)
	if err != nil {
		return err, nil
	}
	s.ts[table] = tts
	return nil, tts
}

func (s *stores) DropTS(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tts, contains := s.ts[table]; contains {
		_ = tts.Drop()
		delete(s.ts, table)
	}
}

func GetKV(table string) (error, kv.KeyValue) {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized"), nil
	}
	return globalStores.GetKV(table)
}

func GetTS(table string) (error, kv.Tskv) {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized"), nil
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
