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

package store

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/db"
	"github.com/lf-edge/ekuiper/internal/pkg/ts"
	kv2 "github.com/lf-edge/ekuiper/pkg/kv"
	"sync"
)

type stores struct {
	kv        map[string]kv2.KeyValue
	ts        map[string]kv2.Tskv
	mu        sync.Mutex
	kvBuilder Builder
	tsBuilder ts.Builder
}

func newStores(db db.Database) (error, *stores) {
	var err error
	var kvBuilder Builder
	var tsBuilder ts.Builder
	kvBuilder, err = CreateStoreBuilder(db)
	if err != nil {
		return err, nil
	}
	err, tsBuilder = ts.CreateTsBuilder(db)
	if err != nil {
		return err, nil
	}
	return nil, &stores{
		kv:        make(map[string]kv2.KeyValue),
		ts:        make(map[string]kv2.Tskv),
		mu:        sync.Mutex{},
		kvBuilder: kvBuilder,
		tsBuilder: tsBuilder,
	}
}

func (s *stores) GetKV(table string) (error, kv2.KeyValue) {
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

func (s *stores) GetTS(table string) (error, kv2.Tskv) {
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

var globalStores *stores = nil

func InitGlobalStores(db db.Database) error {
	var err error
	err, globalStores = newStores(db)
	return err
}

func GetKV(table string) (error, kv2.KeyValue) {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized"), nil
	}
	return globalStores.GetKV(table)
}

func GetTS(table string) (error, kv2.Tskv) {
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
